"""Date extraction with 5-level waterfall.

Ported from ground-news/scripts/pipeline.ts:
  - lines 246-298 (extractDateFromText)
  - lines 432-467 (date waterfall in scrapeArticlePage)
"""

import re
from datetime import datetime, timezone, timedelta

# Sri Lanka is UTC+5:30
_SRI_LANKA_TZ = timezone(timedelta(hours=5, minutes=30))

_MONTHS = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December"
)
_MONTHS_SHORT = "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"

# Pattern 1: "Month DD, YYYY HH:MM am/pm" (Ada Derana English)
_PAT_LONG = re.compile(
    rf"\b({_MONTHS})\s+(\d{{1,2}}),?\s+(\d{{4}})\s+(\d{{1,2}}:\d{{2}}\s*(?:am|pm)?)",
    re.IGNORECASE,
)

# Pattern 2: "Month DD, YYYY" without time
_PAT_DATE_ONLY = re.compile(
    rf"\b({_MONTHS})\s+(\d{{1,2}}),?\s+(\d{{4}})\b",
    re.IGNORECASE,
)

# Pattern 3: "YYYY-MM-DD" or "YYYY.MM.DD" (Sinhala page dates)
_PAT_ISO = re.compile(r"\b(\d{4})[-./](\d{2})[-./](\d{2})\b")

# Pattern 4a: "DD Month YYYY HH:MM am/pm" (Daily Mirror format, e.g. "31 January 2025 11:24 am")
_PAT_DMY_LONG_TIME = re.compile(
    rf"\b(\d{{1,2}})\s+({_MONTHS}|{_MONTHS_SHORT})\s+(\d{{4}})\s+(\d{{1,2}}:\d{{2}}\s*(?:am|pm))",
    re.IGNORECASE,
)

# Pattern 4: "DD Month YYYY" (e.g., "4 February 2026", "05 Feb 2026")
_PAT_DMY_LONG = re.compile(
    rf"\b(\d{{1,2}})\s+({_MONTHS}|{_MONTHS_SHORT})\s+(\d{{4}})\b",
    re.IGNORECASE,
)

# Pattern 5: "DD/MM/YYYY" or "DD-MM-YYYY" (Sri Lankan DMY)
_PAT_DMY = re.compile(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b")

# URL date pattern: /YYYY/MM/DD/
_PAT_URL = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/")

_MIN_YEAR = 2006


def _is_valid_date(dt: datetime) -> bool:
    """Reject epoch dates (≤1970) and future dates (>today + 2 days)."""
    if dt.year < _MIN_YEAR:
        return False
    # Compare in a timezone-consistent way
    if dt.tzinfo:
        now = datetime.now(tz=dt.tzinfo)
    else:
        now = datetime.now(tz=_SRI_LANKA_TZ).replace(tzinfo=None)
    if dt > now + timedelta(days=2):
        return False
    return True


def _safe_parse(date_str: str) -> datetime | None:
    """Try to parse a date string, returning None on failure."""
    # Try common ISO formats first
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            if _is_valid_date(dt):
                return dt if dt.tzinfo else dt.replace(tzinfo=_SRI_LANKA_TZ)
        except ValueError:
            continue

    # Try Python's flexible parsing for RFC 2822 dates (RSS pubDate)
    try:
        from email.utils import parsedate_to_datetime

        dt = parsedate_to_datetime(date_str)
        if _is_valid_date(dt):
            return dt
    except Exception:
        pass

    return None


def extract_date_from_text(text: str) -> datetime | None:
    """Extract a publication date from text content using multiple regex patterns.

    Tries 5 patterns in order, returns first valid match.
    """
    # Pattern 1: "Month DD, YYYY HH:MM am/pm"
    m = _PAT_LONG.search(text)
    if m:
        try:
            dt = datetime.strptime(
                f"{m.group(1)} {m.group(2)}, {m.group(3)} {m.group(4)}",
                "%B %d, %Y %I:%M %p",
            )
            if _is_valid_date(dt):
                return dt.replace(tzinfo=_SRI_LANKA_TZ)
        except ValueError:
            pass

    # Pattern 2: "Month DD, YYYY" without time
    m = _PAT_DATE_ONLY.search(text)
    if m:
        try:
            dt = datetime.strptime(f"{m.group(1)} {m.group(2)}, {m.group(3)}", "%B %d, %Y")
            if _is_valid_date(dt):
                return dt.replace(tzinfo=_SRI_LANKA_TZ)
        except ValueError:
            pass

    # Pattern 3: "YYYY-MM-DD" or "YYYY.MM.DD"
    m = _PAT_ISO.search(text)
    if m:
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=_SRI_LANKA_TZ)
            if _is_valid_date(dt):
                return dt
        except ValueError:
            pass

    # Pattern 4a: "DD Month YYYY HH:MM am/pm" (Daily Mirror)
    m = _PAT_DMY_LONG_TIME.search(text)
    if m:
        try:
            for fmt in ("%d %B %Y %I:%M %p", "%d %b %Y %I:%M %p"):
                try:
                    dt = datetime.strptime(
                        f"{m.group(1)} {m.group(2)} {m.group(3)} {m.group(4).strip()}",
                        fmt,
                    )
                    if _is_valid_date(dt):
                        return dt.replace(tzinfo=_SRI_LANKA_TZ)
                except ValueError:
                    continue
        except ValueError:
            pass

    # Pattern 4: "DD Month YYYY"
    m = _PAT_DMY_LONG.search(text)
    if m:
        try:
            # Try full month name first, then abbreviated
            for fmt in ("%d %B %Y", "%d %b %Y"):
                try:
                    dt = datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}", fmt)
                    if _is_valid_date(dt):
                        return dt.replace(tzinfo=_SRI_LANKA_TZ)
                except ValueError:
                    continue
        except ValueError:
            pass

    # Pattern 5: "DD/MM/YYYY" or "DD-MM-YYYY"
    m = _PAT_DMY.search(text)
    if m:
        try:
            day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            dt = datetime(year, month, day, tzinfo=_SRI_LANKA_TZ)
            if _is_valid_date(dt):
                return dt
        except ValueError:
            pass

    return None


def extract_date_from_url(url: str) -> datetime | None:
    """Extract date from URL path pattern like /2026/02/04/."""
    m = _PAT_URL.search(url)
    if m:
        try:
            dt = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=_SRI_LANKA_TZ)
            if _is_valid_date(dt):
                return dt
        except ValueError:
            pass
    return None


def extract_date_waterfall(
    meta_date: str | None,
    selector_date: str | None,
    url: str,
    body_text: str,
    rss_pub_date: str | None,
) -> datetime | None:
    """5-level date extraction waterfall.

    1. Meta tag ISO date (direct parse)
    2. Selector text (regex extraction)
    3. URL path pattern (/YYYY/MM/DD/)
    4. Body text (regex extraction)
    5. RSS pubDate fallback
    """
    # 1. Meta tag / selector date — try direct ISO parse
    if meta_date:
        dt = _safe_parse(meta_date)
        if dt:
            return dt

    # 2. Selector text — try regex extraction from human-readable text
    if selector_date:
        dt = extract_date_from_text(selector_date)
        if dt:
            return dt

    # 3. URL path pattern
    dt = extract_date_from_url(url)
    if dt:
        return dt

    # 4. Body text — try regex extraction
    if body_text:
        dt = extract_date_from_text(body_text[:3000])
        if dt:
            return dt

    # 5. RSS pubDate fallback
    if rss_pub_date:
        dt = _safe_parse(rss_pub_date)
        if dt:
            return dt

    return None
