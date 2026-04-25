"""Tests for market category classification."""

from src.indexer.market_classifier import classify_market


class TestMarketClassifier:
    def test_macro_fomc(self):
        result = classify_market("Will the Fed cut rates at the March 2026 FOMC meeting?")
        assert result.category == "macro"
        assert "fomc" in result.matched_keywords or "fed " in result.matched_keywords

    def test_macro_cpi(self):
        result = classify_market("Will CPI inflation come in above 3% for February?")
        assert result.category == "macro"

    def test_macro_tags(self):
        result = classify_market("Economic indicator question", tags=["fed", "economy"])
        assert result.category == "macro"

    def test_crypto_weekly(self):
        result = classify_market("Will Bitcoin be above $100k this week?")
        assert result.category == "crypto_weekly"

    def test_crypto_weekly_by_friday(self):
        result = classify_market("Will ETH be above $4000 by Friday?")
        assert result.category == "crypto_weekly"

    def test_crypto_monthly(self):
        result = classify_market("Will BTC reach $120k this month?")
        assert result.category == "crypto_monthly"

    def test_crypto_monthly_excludes_weekly(self):
        result = classify_market("Will BTC be above $100k this week?")
        assert result.category != "crypto_monthly"

    def test_politics(self):
        result = classify_market("Will the Republican candidate win the senate race?")
        assert result.category == "politics"

    def test_geopolitics(self):
        result = classify_market("Will there be a ceasefire in Ukraine by April?")
        assert result.category == "geopolitics"

    def test_other(self):
        result = classify_market("Will it rain in New York tomorrow?")
        assert result.category == "other"

    def test_excluded_5min_crypto(self):
        result = classify_market("Will BTC go up in the next 5 minutes?")
        assert result.category == "excluded"

    def test_excluded_15min_crypto(self):
        result = classify_market("Will ETH be above $4000 in 15 minutes?")
        assert result.category == "excluded"

    def test_manual_override(self):
        result = classify_market(
            "Some ambiguous question",
            category_override="macro",
        )
        assert result.category == "macro"
        assert result.source == "manual"

    def test_ambiguous_dual_match(self):
        # Tags with multiple category matches → ambiguous
        result = classify_market(
            "Will sanctions against Russia lead to a vote in congress?",
            tags=["politics", "geopolitics"],
        )
        assert result.category == "ambiguous"
        assert len(result.all_matched_categories) >= 2

    def test_case_insensitive(self):
        result = classify_market("WILL THE FED CUT RATES AT THE FOMC MEETING?")
        assert result.category == "macro"
