"""City-local resolution helpers for daily weather markets."""

from __future__ import annotations

import re
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from src.indexer.market_classifier import classify_market

MONTH_ABBR = {
    "January": "Jan", "February": "Feb", "March": "Mar", "April": "Apr",
    "May": "May", "June": "Jun", "July": "Jul", "August": "Aug",
    "September": "Sep", "October": "Oct", "November": "Nov", "December": "Dec",
}
MONTH_NUM = {month: i for i, month in enumerate(MONTH_ABBR, start=1)}

# Daily Polymarket weather markets resolve by the named city's local weather date.
WEATHER_CITY_TIMEZONES = {
    "Atlanta": "America/New_York",
    "Buenos Aires": "America/Argentina/Buenos_Aires",
    "Chengdu": "Asia/Shanghai",
    "Denver": "America/Denver",
    "Helsinki": "Europe/Helsinki",
    "Hong Kong": "Asia/Hong_Kong",
    "Jakarta": "Asia/Jakarta",
    "Jeddah": "Asia/Riyadh",
    "Karachi": "Asia/Karachi",
    "Kuala Lumpur": "Asia/Kuala_Lumpur",
    "Lagos": "Africa/Lagos",
    "Los Angeles": "America/Los_Angeles",
    "Madrid": "Europe/Madrid",
    "Manila": "Asia/Manila",
    "Miami": "America/New_York",
    "Milan": "Europe/Rome",
    "New York": "America/New_York",
    "New York City": "America/New_York",
    "Paris": "Europe/Paris",
    "San Francisco": "America/Los_Angeles",
    "Sao Paulo": "America/Sao_Paulo",
    "Seattle": "America/Los_Angeles",
    "Singapore": "Asia/Singapore",
    "Tel Aviv": "Asia/Jerusalem",
    "Tokyo": "Asia/Tokyo",
    "Toronto": "America/Toronto",
    "Washington D.C.": "America/New_York",
    "Washington DC": "America/New_York",
}


def parse_dt(value) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    elif value:
        try:
            text = str(value)
            if len(text) == 10 and text[4] == "-" and text[7] == "-":
                return None
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def effective_market_category(market=None, fallback_title: str | None = None, category: str | None = None) -> str | None:
    category = category or getattr(market, "category_override", None) or getattr(market, "category", None)
    if category == "weather":
        return "weather"
    title = getattr(market, "question", None) or fallback_title or ""
    if title and classify_market(title).category == "weather":
        return "weather"
    return category


def is_weather_market(market=None, fallback_title: str | None = None, category: str | None = None) -> bool:
    return effective_market_category(market, fallback_title, category) == "weather"


def is_generic_weather_end_time(dt: datetime | None) -> bool:
    return bool(dt and dt.hour == 12 and dt.minute == 0 and dt.second == 0)


def extract_weather_city(title: str) -> str | None:
    match = re.search(r"temperature in ([^?]+?) be ", title or "")
    if not match:
        return None
    return match.group(1).strip()


def extract_weather_date(
    title: str,
    pos=None,
    market=None,
    *,
    resolution_time=None,
    now_utc: datetime | None = None,
) -> date | None:
    pos_dt = parse_dt(getattr(pos, "end_date", None))
    if pos_dt is not None:
        return pos_dt.date()

    end_text = str(getattr(pos, "end_date", "") or "")
    if len(end_text) >= 10 and end_text[4] == "-" and end_text[7] == "-":
        try:
            return date.fromisoformat(end_text[:10])
        except ValueError:
            pass

    match = re.search(
        r"\bon (January|February|March|April|May|June|July|August|September|October|November|December) (\d{1,2})\b",
        title or "",
    )
    if not match:
        return None

    year_source = (
        parse_dt(resolution_time)
        or parse_dt(getattr(market, "resolution_time", None))
        or parse_dt(now_utc)
        or datetime.now(timezone.utc)
    )
    month = MONTH_NUM[match.group(1)]
    day = int(match.group(2))
    try:
        return date(year_source.year, month, day)
    except ValueError:
        return None


def weather_local_resolution_cutoff(
    market=None,
    pos=None,
    *,
    title: str | None = None,
    category: str | None = None,
    resolution_time=None,
    now_utc: datetime | None = None,
) -> datetime | None:
    """Return midnight after the weather date in the market city's timezone."""
    title = title or getattr(market, "question", None) or getattr(pos, "title", None) or ""
    if not is_weather_market(market, title, category) or not title:
        return None

    city = extract_weather_city(title)
    if not city:
        return None
    tz_name = WEATHER_CITY_TIMEZONES.get(city)
    if not tz_name:
        return None

    market_date = extract_weather_date(
        title,
        pos,
        market,
        resolution_time=resolution_time,
        now_utc=now_utc,
    )
    if market_date is None:
        return None

    try:
        local_tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        return None
    local_cutoff = datetime.combine(market_date + timedelta(days=1), time.min, tzinfo=local_tz)
    return local_cutoff.astimezone(timezone.utc)


def weather_local_date_gate(
    market,
    pos,
    now_utc: datetime | None = None,
) -> tuple[bool, str, datetime | None] | None:
    """Return weather-market gate by the named city's local calendar date."""
    if not is_weather_market(market):
        return None

    cutoff_utc = weather_local_resolution_cutoff(market, pos, now_utc=now_utc)
    if cutoff_utc is None:
        return None

    now = parse_dt(now_utc) or datetime.now(timezone.utc)
    if now >= cutoff_utc:
        return False, "stale_past_resolution", cutoff_utc
    return True, "weather_local_date_active", cutoff_utc
