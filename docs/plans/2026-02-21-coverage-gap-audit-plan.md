# Coverage Gap Audit Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a `news-agg gaps` CLI command that shows per-source, per-day article coverage with gap detection and CSV export.

**Architecture:** One new DB query function in `db.py`, one new CLI command in `cli.py`. The query uses `generate_series` to build a date grid and `LEFT JOIN` articles to count per source per day. The CLI formats output as a terminal heatmap table or CSV.

**Tech Stack:** asyncpg (SQL query), click (CLI), csv module (export)

---

### Task 1: Apply Database Migration

Run the existing `db-migrate` command to ensure migration 004 (stories table) is applied. This doesn't affect the gap tool directly but is needed for the frontend stories features to work with the restored data.

**Step 1: Run migration**

Run: `news-agg db-migrate`

Expected: `✓ All migrations applied` (004_stories.sql adds stories table + story_id FK)

**Step 2: Verify**

Run:
```bash
docker exec -i news-agg-postgres psql -U newsagg -d newsagg -c "\d articles" | grep story_id
```

Expected: `story_id | uuid | | |` row present

**Step 3: Commit** — No code changes, skip.

---

### Task 2: Add `get_coverage_grid` DB Function

**Files:**
- Modify: `backend/src/news_agg/db.py` (append after `get_story_detail`)

**Step 1: Write the DB function**

Append to `backend/src/news_agg/db.py`:

```python
async def get_coverage_grid(
    pool: asyncpg.Pool,
    since: str,
    until: str,
    source_slug: str | None = None,
) -> list[dict]:
    """Per-source, per-day article counts for a date range.

    Returns rows of {slug, language, date, count}.
    Uses generate_series to include zero-count days.
    """
    conditions = ["TRUE"]
    params: list = [since, until]
    idx = 3

    if source_slug:
        conditions.append(f"s.slug = ${idx}")
        params.append(source_slug)
        idx += 1

    where = " AND ".join(conditions)

    rows = await pool.fetch(
        f"""
        SELECT s.slug, s.language, d.date, COUNT(a.id) as count
        FROM sources s
        CROSS JOIN generate_series($1::date, $2::date, '1 day'::interval) AS d(date)
        LEFT JOIN articles a
            ON a.source_id = s.id
            AND a.published_at::date = d.date
        WHERE s.is_active = true AND {where}
        GROUP BY s.slug, s.language, d.date
        ORDER BY s.slug, d.date
        """,
        *params,
    )
    return [dict(r) for r in rows]
```

**Step 2: Verify syntax**

Run: `cd backend && python -m py_compile src/news_agg/db.py && echo "OK"`

Expected: `OK`

**Step 3: Commit**

```bash
git add backend/src/news_agg/db.py
git commit -m "feat(db): add get_coverage_grid query for gap audit"
```

---

### Task 3: Add `gaps` CLI Command

**Files:**
- Modify: `backend/src/news_agg/cli.py` (add new command before `db-migrate`)

**Step 1: Add the command**

Add these imports at the top of `cli.py` (line ~20, after existing imports):

```python
import csv as csv_mod
from datetime import date as date_type
```

Add the `gaps` command (insert before the `@cli.command("db-migrate")` block around line 1084):

```python
@cli.command()
@click.option("--month", default=None, help="Month to check (YYYY-MM, default: current)")
@click.option("--since", default=None, help="Start date (YYYY-MM-DD)")
@click.option("--until", "until_date", default=None, help="End date (YYYY-MM-DD)")
@click.option("--source", default=None, help="Filter to one source slug")
@click.option("--min-days", default=0, help="Only show sources with >= N gap days")
@click.option("--csv", "csv_file", default=None, help="Export to CSV file")
def gaps(month: str | None, since: str | None, until_date: str | None, source: str | None, min_days: int, csv_file: str | None) -> None:
    """Show per-source, per-day article coverage and highlight gaps."""
    asyncio.run(_gaps(month, since, until_date, source, min_days, csv_file))


async def _gaps(
    month: str | None,
    since: str | None,
    until_date: str | None,
    source: str | None,
    min_days: int,
    csv_file: str | None,
) -> None:
    from news_agg.db import get_coverage_grid, get_pool, close_pool

    # Resolve date range
    if since and until_date:
        start, end = since, until_date
    elif month:
        # Parse YYYY-MM → first and last day
        y, m = int(month[:4]), int(month[5:7])
        start = f"{y:04d}-{m:02d}-01"
        # Last day: next month's 1st minus 1 day
        if m == 12:
            end = f"{y + 1:04d}-01-01"
        else:
            end = f"{y:04d}-{m + 1:02d}-01"
        # Adjust: generate_series is inclusive, but we want last day of month
        end_dt = date_type.fromisoformat(end) - __import__("datetime").timedelta(days=1)
        end = end_dt.isoformat()
    else:
        # Default: current month up to today
        today = date_type.today()
        start = f"{today.year:04d}-{today.month:02d}-01"
        end = today.isoformat()

    pool = await get_pool()
    try:
        rows = await get_coverage_grid(pool, since=start, until=end, source_slug=source)
    finally:
        await close_pool()

    if not rows:
        click.echo("No data found.")
        return

    # CSV export
    if csv_file:
        with open(csv_file, "w", newline="") as f:
            writer = csv_mod.writer(f)
            writer.writerow(["source_slug", "language", "date", "article_count"])
            for r in rows:
                writer.writerow([r["slug"], r["language"], r["date"].isoformat() if hasattr(r["date"], "isoformat") else r["date"], r["count"]])
        click.echo(f"  {GREEN}✓{RESET} Exported {len(rows)} rows to {csv_file}")
        return

    # Build heatmap data structure
    # Collect unique dates and sources
    dates: list[str] = []
    sources_data: dict[str, dict] = {}  # slug → {lang, dates: {date: count}}

    for r in rows:
        d = r["date"].isoformat() if hasattr(r["date"], "isoformat") else str(r["date"])
        if d not in dates:
            dates.append(d)
        slug = r["slug"]
        if slug not in sources_data:
            sources_data[slug] = {"lang": r["language"], "dates": {}}
        sources_data[slug]["dates"][d] = r["count"]

    # Filter by min_days (gap days)
    if min_days > 0:
        sources_data = {
            slug: data
            for slug, data in sources_data.items()
            if sum(1 for d in dates if data["dates"].get(d, 0) == 0) >= min_days
        }

    if not sources_data:
        click.echo("No sources match the filter.")
        return

    # Print header
    click.echo(f"\n{BOLD}Coverage Report: {start} → {end}{RESET}\n")

    # Column widths
    slug_w = max(len(s) for s in sources_data) + 1
    day_labels = [d[-2:] for d in dates]  # Just "01", "02", etc.

    # Header row: source name + day numbers
    header = f"  {'Source':<{slug_w}} │ " + " ".join(f"{dl:>3}" for dl in day_labels) + " │ Total  Avg"
    click.echo(header)
    click.echo(f"  {'─' * slug_w}─┼─" + "─" * (len(dates) * 4) + "┼────────────")

    # Data rows
    for slug in sorted(sources_data):
        data = sources_data[slug]
        cells = []
        total = 0
        gap_days = 0
        for d in dates:
            count = data["dates"].get(d, 0)
            total += count
            if count == 0:
                cells.append(f"{RED}  -{RESET}")
                gap_days += 1
            elif count < 5:
                cells.append(f"{DIM}{count:>3}{RESET}")
            else:
                cells.append(f"{GREEN}{count:>3}{RESET}")
        avg = total / len(dates) if dates else 0
        line = f"  {slug:<{slug_w}} │ " + " ".join(cells) + f" │ {total:>5} {avg:>5.1f}"
        click.echo(line)

    # Summary
    total_all = sum(
        data["dates"].get(d, 0)
        for data in sources_data.values()
        for d in dates
    )
    total_gaps = sum(
        1
        for data in sources_data.values()
        for d in dates
        if data["dates"].get(d, 0) == 0
    )
    click.echo(f"  {'─' * slug_w}─┼─" + "─" * (len(dates) * 4) + "┼────────────")
    click.echo(
        f"\n  {BOLD}Total:{RESET} {total_all:,} articles across {len(sources_data)} sources, "
        f"{len(dates)} days"
    )
    click.echo(
        f"  {BOLD}Gaps:{RESET} {total_gaps} source-days with zero articles "
        f"({total_gaps}/{len(sources_data) * len(dates)} = "
        f"{total_gaps / (len(sources_data) * len(dates)) * 100:.0f}%)\n"
    )
```

**Step 2: Update CLI docstring**

Add to the docstring at the top of `cli.py`:

```
    news-agg gaps --month 2026-02
```

**Step 3: Verify syntax**

Run: `cd backend && python -m py_compile src/news_agg/cli.py && echo "OK"`

Expected: `OK`

**Step 4: Commit**

```bash
git add backend/src/news_agg/cli.py
git commit -m "feat(cli): add gaps command for coverage gap audit"
```

---

### Task 4: Write Tests

**Files:**
- Create: `backend/tests/test_gaps.py`

**Step 1: Write tests for date range resolution and data formatting**

```python
"""Tests for the gaps coverage audit tool."""
from datetime import date


def test_month_to_date_range():
    """--month YYYY-MM resolves to first and last day of month."""
    month = "2026-02"
    y, m = int(month[:4]), int(month[5:7])
    start = f"{y:04d}-{m:02d}-01"
    assert start == "2026-02-01"

    # Last day of Feb 2026 (not a leap year)
    if m == 12:
        end_str = f"{y + 1:04d}-01-01"
    else:
        end_str = f"{y:04d}-{m + 1:02d}-01"
    from datetime import timedelta
    end = (date.fromisoformat(end_str) - timedelta(days=1)).isoformat()
    assert end == "2026-02-28"


def test_month_december():
    """December wraps to next year correctly."""
    month = "2025-12"
    y, m = int(month[:4]), int(month[5:7])
    start = f"{y:04d}-{m:02d}-01"
    assert start == "2025-12-01"

    if m == 12:
        end_str = f"{y + 1:04d}-01-01"
    else:
        end_str = f"{y:04d}-{m + 1:02d}-01"
    from datetime import timedelta
    end = (date.fromisoformat(end_str) - timedelta(days=1)).isoformat()
    assert end == "2025-12-31"


def test_leap_year_feb():
    """Feb in leap year (2024) has 29 days."""
    month = "2024-02"
    y, m = int(month[:4]), int(month[5:7])
    if m == 12:
        end_str = f"{y + 1:04d}-01-01"
    else:
        end_str = f"{y:04d}-{m + 1:02d}-01"
    from datetime import timedelta
    end = (date.fromisoformat(end_str) - timedelta(days=1)).isoformat()
    assert end == "2024-02-29"


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
```

**Step 2: Run tests**

Run: `cd backend && uv run pytest tests/test_gaps.py -v`

Expected: All 5 tests pass

**Step 3: Commit**

```bash
git add backend/tests/test_gaps.py
git commit -m "test: add unit tests for gaps coverage audit"
```

---

### Task 5: Smoke Test Against Live Database

**Step 1: Run migration**

Run: `news-agg db-migrate`

**Step 2: Run the gaps command for February 2026**

Run: `news-agg gaps --month 2026-02`

Expected: Heatmap showing all 23 sources with daily article counts for Feb 1–21 (today). Most sources will show zeros for Feb 17–21. Large sources (ada-derana) should show 30-60+/day on active days.

**Step 3: Test CSV export**

Run: `news-agg gaps --month 2026-02 --csv /tmp/feb-gaps.csv && head -5 /tmp/feb-gaps.csv`

Expected: CSV with headers `source_slug,language,date,article_count` and data rows.

**Step 4: Test source filter**

Run: `news-agg gaps --month 2026-02 --source ada-derana-en`

Expected: Single-source heatmap showing only ada-derana-en.

**Step 5: Test min-days filter**

Run: `news-agg gaps --month 2026-02 --min-days 10`

Expected: Only sources with 10+ gap days shown.

**Step 6: Commit all together**

```bash
git add -A
git commit -m "feat: coverage gap audit tool (news-agg gaps)"
```

---

### Task 6: Push

```bash
git push
```
