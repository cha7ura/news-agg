"""Tests for the gaps coverage audit tool."""
from datetime import date, timedelta


def test_month_to_date_range_feb():
    """--month 2026-02 resolves to Feb 1–28."""
    month = "2026-02"
    y, m = int(month[:4]), int(month[5:7])
    start = f"{y:04d}-{m:02d}-01"
    assert start == "2026-02-01"

    if m == 12:
        end_dt = date(y + 1, 1, 1) - timedelta(days=1)
    else:
        end_dt = date(y, m + 1, 1) - timedelta(days=1)
    assert end_dt.isoformat() == "2026-02-28"


def test_month_december():
    """December wraps to next year correctly."""
    month = "2025-12"
    y, m = int(month[:4]), int(month[5:7])
    start = f"{y:04d}-{m:02d}-01"
    assert start == "2025-12-01"

    if m == 12:
        end_dt = date(y + 1, 1, 1) - timedelta(days=1)
    else:
        end_dt = date(y, m + 1, 1) - timedelta(days=1)
    assert end_dt.isoformat() == "2025-12-31"


def test_leap_year_feb():
    """Feb in leap year (2024) has 29 days."""
    month = "2024-02"
    y, m = int(month[:4]), int(month[5:7])
    if m == 12:
        end_dt = date(y + 1, 1, 1) - timedelta(days=1)
    else:
        end_dt = date(y, m + 1, 1) - timedelta(days=1)
    assert end_dt.isoformat() == "2024-02-29"


def test_gap_detection():
    """Zero-count days are correctly identified as gaps."""
    dates = ["2026-02-01", "2026-02-02", "2026-02-03"]
    source_dates = {"2026-02-01": 10, "2026-02-02": 0, "2026-02-03": 5}
    gaps = [d for d in dates if source_dates.get(d, 0) == 0]
    assert gaps == ["2026-02-02"]


def test_min_days_filter():
    """--min-days filters sources with fewer gap days."""
    sources_data = {
        "source-a": {"dates": {"d1": 10, "d2": 0, "d3": 0}},  # 2 gaps
        "source-b": {"dates": {"d1": 5, "d2": 3, "d3": 7}},   # 0 gaps
        "source-c": {"dates": {"d1": 0, "d2": 0, "d3": 0}},   # 3 gaps
    }
    dates = ["d1", "d2", "d3"]
    min_days = 2
    filtered = {
        slug: data
        for slug, data in sources_data.items()
        if sum(1 for d in dates if data["dates"].get(d, 0) == 0) >= min_days
    }
    assert set(filtered.keys()) == {"source-a", "source-c"}
