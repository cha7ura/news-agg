from datetime import datetime, timedelta, timezone

from news_agg.text.dates import extract_date_from_text, extract_date_from_url, extract_date_waterfall

SRI_LANKA_TZ = timezone(timedelta(hours=5, minutes=30))


def test_ada_derana_format():
    """'February 4, 2026 02:39 pm' — the primary Ada Derana English date format."""
    dt = extract_date_from_text("February 4, 2026 02:39 pm")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 2
    assert dt.day == 4


def test_month_day_year_no_time():
    dt = extract_date_from_text("January 15, 2026")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 1
    assert dt.day == 15


def test_iso_format():
    dt = extract_date_from_text("Published on 2026-02-14")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 2
    assert dt.day == 14


def test_dmy_long_format():
    dt = extract_date_from_text("4 February 2026")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 2
    assert dt.day == 4


def test_dmy_short_month():
    dt = extract_date_from_text("05 Feb 2026")
    assert dt is not None
    assert dt.month == 2
    assert dt.day == 5


def test_dmy_slash_format():
    """Sri Lankan DD/MM/YYYY format."""
    dt = extract_date_from_text("14/02/2026")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 2
    assert dt.day == 14


def test_url_date_extraction():
    dt = extract_date_from_url("https://example.com/2026/02/04/some-article")
    assert dt is not None
    assert dt.year == 2026
    assert dt.month == 2
    assert dt.day == 4


def test_url_no_date():
    assert extract_date_from_url("https://example.com/news/12345") is None


def test_invalid_date_returns_none():
    assert extract_date_from_text("No date here") is None
    assert extract_date_from_text("") is None


def test_old_dates_rejected():
    """Dates before 2006 should be rejected."""
    assert extract_date_from_text("January 1, 2000") is None


def test_waterfall_meta_date():
    """Meta tag ISO date should be tried first."""
    dt = extract_date_waterfall(
        meta_date="2026-02-04T14:39:00+05:30",
        selector_date=None,
        url="https://example.com/news/12345",
        body_text="",
        rss_pub_date=None,
    )
    assert dt is not None
    assert dt.year == 2026


def test_waterfall_rss_fallback():
    """RSS pubDate should be the last resort."""
    dt = extract_date_waterfall(
        meta_date=None,
        selector_date=None,
        url="https://example.com/news/12345",
        body_text="Nothing useful",
        rss_pub_date="Tue, 04 Feb 2026 14:39:00 +0530",
    )
    assert dt is not None
    assert dt.year == 2026


def test_daily_mirror_date_format():
    """'31 January 2025 11:24 am' — Daily Mirror's exact published date format."""
    dt = extract_date_from_text("31 January 2025 11:24 am")
    assert dt is not None
    assert dt.year == 2025
    assert dt.month == 1
    assert dt.day == 31
    assert dt.hour == 11
    assert dt.minute == 24


def test_future_date_rejected():
    """Dates more than 2 days in the future should be rejected."""
    future = "1 January 2099 10:00 am"
    assert extract_date_from_text(future) is None


def test_epoch_date_rejected():
    """Epoch/1970 dates should be rejected (Daily Mirror sometimes returns these)."""
    assert extract_date_from_text("1 January 1970") is None
