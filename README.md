# News Aggregator

Async news scraping pipeline for Sri Lankan media. Collects articles from multiple sources, stores in PostgreSQL (local Docker or Supabase).

## Sources

| Source | Language | Method |
|--------|----------|--------|
| Ada Derana | English | RSS + NID sweep |
| Ada Derana Sinhala | Sinhala | Listing page + NID sweep |
| NewsFirst | English | Listing page + date sweep |
| The Island | English | RSS + NID sweep |
| EconomyNext | English | RSS + NID sweep |
| Colombo Gazette | English | Date sweep |
| News19 | Sinhala | RSS + NID sweep |

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Docker & Docker Compose

## Setup

### 1. Start Docker services

```bash
docker compose up -d
```

This starts:
- **PostgreSQL 16** on port 5432 (auto-creates schema via `docker/init.sql`)
- **Playwright** browser server on port 3100
- **pgAdmin** on port 5050 (login: `admin@newsagg.dev` / `admin`)

### 2. Configure environment

```bash
cp .env.example backend/.env
```

Edit `backend/.env`:
```env
DATABASE_URL=postgresql://newsagg:newsagg@localhost:5432/newsagg
SUPABASE_DATABASE_URL=postgresql://postgres.[ref]:[password]@db.[ref].supabase.co:5432/postgres
PLAYWRIGHT_WS_URL=ws://localhost:3100
```

### 3. Install dependencies

```bash
cd backend
uv sync
```

## Usage

### Regular ingestion (latest articles from listing pages)

```bash
uv run news-agg ingest --source ada-derana-en --limit 20
uv run news-agg ingest --source ada-derana-si --limit 20
uv run news-agg ingest --source newsfirst-en --limit 20
uv run news-agg ingest --source island-en --limit 20
uv run news-agg ingest --source economynext-en --limit 20
uv run news-agg ingest --source colombo-gazette-en --limit 20
uv run news-agg ingest --source news19-si --limit 20
```

### Backfill — Ada Derana NID sweep

Iterates through sequential article IDs for full historical coverage:

```bash
# English (nid 10000 → 119000)
uv run news-agg ingest --nid-sweep --source ada-derana-en --concurrency 5

# Sinhala (nid 10000 → 222000)
uv run news-agg ingest --nid-sweep --source ada-derana-si --concurrency 5
```

### Backfill — NewsFirst date sweep

Iterates through daily archive pages (`/YYYY/MM/DD`, content available from 2020):

```bash
# Full sweep from 2020
uv run news-agg ingest --source newsfirst-en --date-sweep --concurrency 5

# Last 30 days only
uv run news-agg ingest --source newsfirst-en --date-sweep --days 30 --concurrency 3
```

### Backfill — NID sweep (The Island, EconomyNext, News19)

WordPress `?p=ID` redirect sweeps for full historical coverage:

```bash
uv run news-agg ingest --nid-sweep --source island-en --concurrency 5
uv run news-agg ingest --nid-sweep --source economynext-en --concurrency 5
uv run news-agg ingest --nid-sweep --source news19-si --concurrency 5
```

### Backfill — Archive page crawl

Paginated archive scraping:

```bash
uv run news-agg ingest --source ada-derana-en --backfill --pages 40 --concurrency 5
uv run news-agg ingest --source island-en --backfill --pages 40 --concurrency 5
uv run news-agg ingest --source economynext-en --backfill --pages 100 --concurrency 5
uv run news-agg ingest --source news19-si --backfill --pages 40 --concurrency 5
```

### Backfill — Date sweep (Colombo Gazette)

Calendar-based archive crawl (from 2020):

```bash
uv run news-agg ingest --source colombo-gazette-en --date-sweep --concurrency 5
```

### Run all backfills concurrently (Supabase)

```bash
# Phase 1: Archive/date sweeps (faster first pass)
uv run news-agg ingest --nid-sweep --source ada-derana-en --concurrency 5 --supabase &
uv run news-agg ingest --nid-sweep --source ada-derana-si --concurrency 5 --supabase &
uv run news-agg ingest --source newsfirst-en --date-sweep --concurrency 5 --supabase &
uv run news-agg ingest --source island-en --backfill --pages 40 --concurrency 5 --supabase &
uv run news-agg ingest --source economynext-en --backfill --pages 100 --concurrency 5 --supabase &
uv run news-agg ingest --source colombo-gazette-en --date-sweep --concurrency 5 --supabase &
uv run news-agg ingest --source news19-si --backfill --pages 40 --concurrency 5 --supabase &
wait

# Phase 2: NID sweeps (fills gaps after archive sweeps)
uv run news-agg ingest --nid-sweep --source island-en --concurrency 5 --supabase &
uv run news-agg ingest --nid-sweep --source economynext-en --concurrency 5 --supabase &
uv run news-agg ingest --nid-sweep --source news19-si --concurrency 5 --supabase &
wait
```

### Using Supabase

Add `--supabase` to any command to target the Supabase database:

```bash
uv run news-agg ingest --source ada-derana-en --limit 20 --supabase
uv run news-agg ingest --nid-sweep --source ada-derana-en --concurrency 5 --supabase
uv run news-agg check --supabase
```

### Migrate local data to Supabase

```bash
uv run news-agg migrate
```

### Backup Supabase to local

```bash
uv run news-agg backup
```

### Check database stats

```bash
uv run news-agg check              # local DB
uv run news-agg check --supabase   # Supabase
```

## Running tests

```bash
cd backend
uv run pytest tests/ -v
```

## Project structure

```
backend/
  src/news_agg/
    cli.py           # Click CLI entry point
    config.py         # Pydantic settings (env vars)
    db.py             # asyncpg database operations
    pipeline.py       # Main ingestion pipeline
    backfill.py       # Archive, NID sweep, date sweep
    models.py         # Pydantic models
    sources.yaml      # Per-source scraping config
    source_config.py  # YAML config loader
    scraper/
      article.py      # Article page scraper
      browser.py      # Playwright connection
      listing.py      # Listing page link extraction
      rss.py           # RSS feed parser
    text/
      dates.py         # Date extraction (5-level waterfall)
      dedup.py         # Title-based dedup
      normalize.py     # Text normalization
      language.py      # Language detection
    utils/
      logging.py       # Colored logging
      rate_limit.py    # Async rate limiter
docker/
  init.sql            # PostgreSQL schema
docker-compose.yml    # Postgres, Playwright, pgAdmin
```
