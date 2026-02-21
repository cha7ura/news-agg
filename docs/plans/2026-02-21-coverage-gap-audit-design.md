# Coverage Gap Audit Tool

**Date:** 2026-02-21
**Status:** Approved

## Problem

107K articles across 23 sources, but coverage has significant date gaps. The last 5 days (Feb 17–21) show zero articles across all sources. No systematic way to identify which sources are missing data on which dates.

## Solution

A `news-agg gaps` CLI command that generates a per-source, per-day coverage heatmap and identifies missing dates. Start with February 2026 for all 23 sources, learn patterns, then expand.

## Design

### CLI: `news-agg gaps`

**Options:**
- `--month YYYY-MM` — Show coverage for a month (default: current month)
- `--source SLUG` — Filter to one source
- `--since YYYY-MM-DD` / `--until YYYY-MM-DD` — Custom date range
- `--min-days N` — Only show sources with ≥N gap days
- `--csv FILE` — Export raw data as CSV (source_slug, date, article_count)

**Output:** Terminal heatmap table showing article counts per source per day, with zero-days highlighted. Summary row with total articles and average per day.

**Implementation:** Single SQL query using `generate_series` for date grid, `LEFT JOIN` to articles, `GROUP BY` source + date. Pure read-only reporting.

### Prerequisites

1. Run `news-agg db-migrate` to apply migration 004 (stories table + story_id FK)

### Workflow

1. Run migration
2. Run `news-agg gaps --month 2026-02` to see the full picture
3. Identify patterns (consistent daily output vs sporadic)
4. Fill gaps using existing backfill commands per-source
5. Re-run report to verify
6. Expand to January 2026, then deeper history

### Source Backfill Capabilities (all 23)

| Source | Backfill Methods |
|--------|-----------------|
| ada-derana-en/si | archive + NID sweep |
| daily-mirror-en | archive (100 pages) |
| newsfirst-en | date_sweep |
| colombo-gazette-en | date_sweep |
| economynext-en, island-en, lanka-news-web-en, news19-si, sunday-observer-en, lankatruth-si, lakresa-si, newsnow-si | archive + NID sweep |
| daily-news-en, dinamina-si, news-lk-en | archive only |
| ada-si, colombo-telegraph-en, deshaya-si, divaina-si, ft-en, hiru-news-en, lankadeepa-si | NID sweep only |

### Data Context (as of Feb 21 snapshot)

- 107,198 total articles, 0 reviewed
- Big 5 sources: ada-derana-en (47K), ada-derana-si (30K), newsfirst-en (9K), lankatruth-si (8K), economynext-en (5K)
- 18 smaller sources: 2–50 articles each
- 19,478 dead links across 10 sources
- No data from Feb 17–21 across all sources
