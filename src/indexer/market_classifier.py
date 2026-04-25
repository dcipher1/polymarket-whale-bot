"""Market category classification using Polymarket event categories + keyword fallback."""

import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Polymarket event category → our internal category
# Case-insensitive lookup (keys are lowercased at match time)
POLYMARKET_CATEGORY_MAP: dict[str, str] = {
    # Broad categories
    "politics": "politics",
    "us-current-affairs": "politics",
    "geopolitics": "geopolitics",
    "world": "geopolitics",
    "crypto": "crypto_weekly",  # refined to crypto_monthly by keyword check
    "finance": "macro",
    "economy": "macro",
    "business": "macro",
    "stocks": "macro",
    "ipos": "macro",
    "sports": "sports",
    "tech": "tech",
    "weather": "weather",
    "culture": "culture",
    "pop culture": "culture",
    "entertainment": "culture",
    # Event-level tags from Polymarket
    "commodities": "macro",
    "oil": "macro",
    "ipo": "macro",
    "ipos": "macro",
    "big tech": "macro",
    "bonds": "macro",
    "treasury": "macro",
    "forex": "macro",
    "gold": "macro",
    "silver": "macro",
    "russia": "geopolitics",
    "ukraine": "geopolitics",
    "china": "geopolitics",
    "nato": "geopolitics",
    "eu": "geopolitics",
    "india": "geopolitics",
    "iran": "geopolitics",
    "israel": "geopolitics",
    "greenland": "geopolitics",
    "france": "politics",
    "uk": "politics",
    "england": "politics",
    "elon musk": "macro",
    "bitcoin": "crypto_weekly",
    "ethereum": "crypto_weekly",
    "solana": "crypto_weekly",
    "microstrategy": "crypto_monthly",
    "exchange": "crypto_weekly",
    "defi": "crypto_weekly",
    "nft": "crypto_weekly",
}

CATEGORY_RULES: dict[str, dict] = {
    "macro": {
        "tags": [
            "fed", "fomc", "cpi", "inflation", "unemployment", "gdp", "pce",
            "payrolls", "jobs", "recession", "interest-rates", "economy",
        ],
        "keywords": [
            "fed ", "fomc", "cpi", "inflation", "unemployment", "gdp",
            "payroll", "recession", "rate cut", "rate hike", "basis point",
            "central bank", "ecb", "boe", "boj", "snb", "rba",
            "pce", "core inflation", "jobs report", "nonfarm",
        ],
    },
    "crypto_weekly": {
        "keywords": ["this week", "weekly", "by friday", "by sunday", "end of week"],
        "must_also_match": [
            "btc", "bitcoin", "eth", "ethereum", "sol", "solana",
            "xrp", "doge", "crypto",
        ],
    },
    "crypto_monthly": {
        "keywords": [
            "this month", "monthly", "by end of", "in march", "in april",
            "in may", "in june", "in july", "in august", "in september",
            "in october", "in november", "in december", "in january", "in february",
            "highest price", "lowest price",
        ],
        "must_also_match": [
            "btc", "bitcoin", "eth", "ethereum", "sol", "solana",
            "xrp", "doge", "crypto",
        ],
        "must_not_match": ["this week", "weekly"],
    },
    "politics": {
        "tags": [
            "politics", "election", "congress", "senate", "house",
            "midterms", "primaries", "governor",
        ],
        "keywords": [
            "win ", "elected", "nomination", "primary", "vote",
            "senate", "house", "congress", "governor", "president",
            "republican", "democrat", "midterm", "ballot",
        ],
    },
    "geopolitics": {
        "tags": ["geopolitics", "world", "war", "conflict"],
        "keywords": [
            "iran", "ukraine", "russia", "china", "ceasefire", "strike",
            "sanctions", "invasion", "regime", "strait of hormuz",
            "nato", "tariff", "trade war", "coup",
            "north korea", "taiwan", "south china sea",
            "houthi", "hezbollah", "hamas", "gaza",
            "nuclear", "missile", "military", "peace deal",
            "un security council", "brics", "opec",
        ],
    },
}

# Sports patterns — if these match, override any politics/geopolitics classification
SPORTS_PATTERNS = [
    r"\bwin on 2\d{3}-\d{2}-\d{2}\b",      # "win on 2026-03-15" (dated match)
    r"\bwin on \d{4}-\d{2}-\d{2}\b",
    r"\b(FC|AC|SC|SK|CF|CD|BV|SV|RC|RCD)\b",  # Football club prefixes
    r"\bvs\.?\b",                           # "X vs Y" format
    r"\bend in a draw\b",
    r"\bwin the \d{4}[-–]\d{2,4}\b",       # "win the 2025-2026 NHL"
    r"\b(NBA|NFL|NHL|MLB|UFC|FIFA|ATP|WTA|MLS|EPL|LaLiga|Bundesliga|Serie A|Ligue 1|Premier League)\b",
    r"\b(Stanley Cup|Super Bowl|World Series|Champions League|Europa League|World Cup)\b",
    r"\b(KO|TKO|knockout|submission|decision)\b",
    r"\b(Quakers|Bulldogs|Lakers|Celtics|Warriors|Clippers|Mavericks|Rockets|Kings)\b",
    r"\b(Masters tournament|Grand Slam|Roland Garros|Wimbledon|US Open tennis)\b",
    r"\b(Rookie of the Year|MVP|Ballon d'Or|Art Ross|Hart Memorial|Richard Trophy)\b",
    r"\bwin the (Atlantic|Pacific|Central|Metropolitan|Eastern|Western) (Division|Conference)\b",
    r"\b(BO3|BO5)\b",                      # Esports best-of format
    r"\bCounter-Strike\b",
    r"\bEurovision\b",
]

# Patterns that indicate ultra-short-term crypto markets to exclude
EXCLUDED_PATTERNS = [
    r"\b5[- ]?min(ute)?s?\b",
    r"\b15[- ]?min(ute)?s?\b",
    r"\b1[- ]?hour\b",
    r"\bhourly\b",
]


@dataclass
class ClassificationResult:
    category: str  # "macro", "crypto_weekly", "other", "excluded", "ambiguous"
    matched_keywords: list[str] = field(default_factory=list)
    matched_tags: list[str] = field(default_factory=list)
    source: str = "auto"
    all_matched_categories: list[str] = field(default_factory=list)


def classify_market(
    question: str,
    tags: list[str] | None = None,
    category_override: str | None = None,
    polymarket_category: str | None = None,
) -> ClassificationResult:
    """Classify a market into a category.

    Priority: manual override > Polymarket event category > keyword fallback.
    """

    if category_override:
        return ClassificationResult(
            category=category_override,
            source="manual",
        )

    question_lower = question.lower()
    tag_labels = [t.lower() for t in (tags or [])]

    # Check for sports — classify as sports instead of falling through to other rules
    for pattern in SPORTS_PATTERNS:
        if re.search(pattern, question, re.IGNORECASE):
            return ClassificationResult(category="sports")

    # Check for excluded ultra-short crypto (before Polymarket category, so 5-min
    # crypto markets don't slip through as crypto_weekly)
    for pattern in EXCLUDED_PATTERNS:
        if re.search(pattern, question_lower):
            has_crypto = any(
                kw in question_lower
                for kw in ["btc", "bitcoin", "eth", "ethereum", "sol", "solana", "crypto"]
            )
            if has_crypto:
                return ClassificationResult(category="excluded")

    # Primary: use Polymarket's own event category if available
    if polymarket_category:
        mapped = POLYMARKET_CATEGORY_MAP.get(polymarket_category.lower())
        if mapped:
            # For crypto, refine weekly vs monthly using keywords
            if mapped == "crypto_weekly":
                monthly_kws = CATEGORY_RULES.get("crypto_monthly", {}).get("keywords", [])
                if any(kw in question_lower for kw in monthly_kws):
                    mapped = "crypto_monthly"
            return ClassificationResult(
                category=mapped,
                source="polymarket_event",
                matched_tags=[polymarket_category],
            )

    # Secondary: check event-level tags against our category map
    # Polymarket puts rich tags on events (e.g. "Commodities", "Oil", "Finance")
    if tag_labels:
        tag_matches = []
        for tag in tag_labels:
            mapped = POLYMARKET_CATEGORY_MAP.get(tag.lower())
            if mapped and mapped != "other":
                tag_matches.append((tag, mapped))
        if tag_matches:
            unique_cats = set(cat for _, cat in tag_matches)
            if len(unique_cats) == 1:
                best_cat = unique_cats.pop()
                if best_cat == "crypto_weekly":
                    monthly_kws = CATEGORY_RULES.get("crypto_monthly", {}).get("keywords", [])
                    if any(kw in question_lower for kw in monthly_kws):
                        best_cat = "crypto_monthly"
                return ClassificationResult(
                    category=best_cat,
                    source="polymarket_tag",
                    matched_tags=[t for t, _ in tag_matches],
                )
            else:
                return ClassificationResult(
                    category="ambiguous",
                    source="polymarket_tag_ambiguous",
                    matched_tags=[t for t, _ in tag_matches],
                    all_matched_categories=list(unique_cats),
                )

    # Fallback: keyword/tag matching
    matched_categories: dict[str, ClassificationResult] = {}

    for category, rules in CATEGORY_RULES.items():
        kw_matches = []
        tag_matches = []

        # Tag matching
        for rule_tag in rules.get("tags", []):
            if rule_tag in tag_labels:
                tag_matches.append(rule_tag)

        # Keyword matching
        for keyword in rules.get("keywords", []):
            if keyword in question_lower:
                kw_matches.append(keyword)

        has_match = bool(kw_matches) or bool(tag_matches)

        if not has_match:
            continue

        # Check must_also_match constraint
        must_also = rules.get("must_also_match", [])
        if must_also:
            if not any(m in question_lower for m in must_also):
                continue

        # Check must_not_match constraint
        must_not = rules.get("must_not_match", [])
        if must_not:
            if any(m in question_lower for m in must_not):
                continue

        matched_categories[category] = ClassificationResult(
            category=category,
            matched_keywords=kw_matches,
            matched_tags=tag_matches,
            source="auto",
        )

    if not matched_categories:
        return ClassificationResult(category="other")

    if len(matched_categories) == 1:
        result = list(matched_categories.values())[0]
        result.all_matched_categories = list(matched_categories.keys())
        return result

    # Multiple categories matched — ambiguous
    all_cats = list(matched_categories.keys())
    all_kw = []
    all_tags = []
    for r in matched_categories.values():
        all_kw.extend(r.matched_keywords)
        all_tags.extend(r.matched_tags)

    logger.debug(
        "Ambiguous classification for '%s': matched %s", question[:80], all_cats
    )
    return ClassificationResult(
        category="ambiguous",
        matched_keywords=all_kw,
        matched_tags=all_tags,
        source="auto",
        all_matched_categories=all_cats,
    )
