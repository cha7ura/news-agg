# News Aggregator

End-to-end news pipeline for Sri Lankan media. Scrapes articles from 20+ sources, runs LLM quality review, builds a knowledge graph, and provides full-text search — orchestrated by an autonomous LangGraph agent.

## Pipeline

```
                Ingest                    Review                   Graph                  Search
          ┌──────────────┐         ┌──────────────┐        ┌──────────────┐        ┌──────────────┐
 RSS/     │ Discover URLs│  LLM    │ QA pass/fail │ Neo4j  │  Graphiti    │ Meili  │  Full-text   │
 Listing ─│ Dedup + scrape├────────│ Categorize   ├────────│  Entities &  ├────────│  Search API  │
 Pages    │ Store to PG  │         │ Summarize    │        │  Relations   │        │  Faceted     │
          └──────────────┘         └──────────────┘        └──────────────┘        └──────────────┘
                │                         │                        │                       │
                └─────────────────────────┴────────────────────────┴───────────────────────┘
                                          │
                                ┌─────────┴──────────┐
                                │  LangGraph Agent   │
                                │  (orchestrates all) │
                                └────────────────────┘
```

### What each stage does

| Stage | Description | Data store |
|-------|-------------|------------|
| **Ingest** | Discovers article URLs via RSS or listing pages, deduplicates against DB, scrapes in parallel with Playwright, extracts content/date/author | PostgreSQL |
| **Review** | LLM (OpenRouter) rates quality (pass/warn/fail, 1-10 score), extracts category, entities, location, English summary | PostgreSQL |
| **Graph** | Saves QA-passed articles to Neo4j via Graphiti — auto-extracts entities and relationships | Neo4j |
| **Search** | Syncs articles from PostgreSQL to Meilisearch — full-text search with source/language/category facets | Meilisearch |

## Sources

| Source | Slug | Language | Method | Cloudflare |
|--------|------|----------|--------|------------|
| Ada Derana | `ada-derana-en` | English | RSS + NID sweep + archive | No |
| Ada Derana Sinhala | `ada-derana-si` | Sinhala | Listing + NID sweep + archive | No |
| Daily Mirror | `daily-mirror-en` | English | Listing + archive | Yes |
| NewsFirst | `newsfirst-en` | English | Listing + date sweep | No |
| The Island | `island-en` | English | RSS + NID sweep + archive | No |
| EconomyNext | `economynext-en` | English | RSS + NID sweep + archive | No |
| Colombo Gazette | `colombo-gazette-en` | English | Date sweep | No |
| News19 | `news19-si` | Sinhala | RSS + NID sweep + archive | No |
| Sunday Observer | `sunday-observer-en` | English | RSS + archive | No |
| Lanka News Web | `lanka-news-web-en` | English | RSS + archive | No |
| Lanka Truth | `lankatruth-si` | Sinhala | Listing | No |

Plus 12 more configured in `docker/init.sql` (Hiru News, Daily FT, Lankadeepa, etc.)

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) package manager
- Docker & Docker Compose

## Setup

### 1. Start Docker services

```bash
# Core services (PostgreSQL, Playwright, pgAdmin)
docker compose up -d

# Full stack (add Meilisearch, Neo4j, SearXNG)
docker compose up -d postgres playwright meilisearch neo4j searxng pgadmin
```

Services:

| Service | Port | Purpose |
|---------|------|---------|
| PostgreSQL 16 | 5432 | Article storage (source of truth) |
| Playwright v1.58.0 | 3100 | Headless browser for scraping |
| Neo4j 5.26 | 7474/7687 | Knowledge graph (Graphiti) |
| Meilisearch v1.12 | 7700 | Full-text search engine |
| SearXNG | 8888 | Web search for article enrichment |
| pgAdmin 4 | 5050 | Database UI (`admin@newsagg.dev` / `admin`) |
| Tor proxy | 9050/8118 | SOCKS5/HTTP proxy (optional) |

### 2. Configure environment

```bash
cp .env.example backend/.env
```

Edit `backend/.env`:
```env
DATABASE_URL=postgresql://newsagg:newsagg@localhost:5432/newsagg
PLAYWRIGHT_WS_URL=ws://localhost:3100
LOG_LEVEL=info
RATE_LIMIT_MS=2000

# LLM for review/agent (OpenRouter)
OPENROUTER_API_KEY=sk-or-v1-...
OPENROUTER_MODEL=openrouter/aurora-alpha

# Observability (optional)
LANGFUSE_SECRET_KEY=sk-lf-...
LANGFUSE_PUBLIC_KEY=pk-lf-...
LANGFUSE_BASE_URL=https://us.cloud.langfuse.com

# Knowledge graph
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=newsagg_neo4j

# R2 snapshots (Cloudflare)
R2_ENDPOINT_URL=https://....r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_BUCKET_NAME=newsagg-snapshots

# Supabase (optional remote DB)
SUPABASE_DATABASE_URL=postgresql://postgres.[ref]:[password]@...supabase.com:5432/postgres
```

### 3. Install dependencies

```bash
cd backend
uv sync
```

## Usage

### Autonomous agent (recommended)

The LangGraph agent runs the full pipeline autonomously — ingest, review, graph save:

```bash
# Full cycle: ingest → review → graph
uv run news-agg agent run

# Target specific sources
uv run news-agg agent run --sources ada-derana-en,island-en --limit 20

# Ingest only (no review/graph)
uv run news-agg agent run --run-type ingest_only

# View run history
uv run news-agg agent history

# Inspect a specific run
uv run news-agg agent inspect <run-id>
```

The agent decides which sources need ingestion, reviews unprocessed articles, enriches high-value articles via web search (SearXNG), saves passing articles to the knowledge graph, and logs everything to the `agent_runs` table.

### Manual CLI commands

#### Ingest (scrape articles)

```bash
# Single source
uv run news-agg ingest --source ada-derana-en --limit 20

# All sources (intelligent multi-source scheduling)
uv run news-agg ingest --limit 20 --concurrency 5

# With Supabase instead of local DB
uv run news-agg ingest --source ada-derana-en --limit 20 --supabase
```

#### Backfill (historical data)

```bash
# Archive page crawl
uv run news-agg ingest --source ada-derana-en --backfill --pages 40 --concurrency 5

# NID sweep (sequential article IDs)
uv run news-agg ingest --nid-sweep --source ada-derana-en --concurrency 5

# Date sweep (calendar-based archives)
uv run news-agg ingest --source newsfirst-en --date-sweep --days 30 --concurrency 3

# Auto-backfill (runs all configured methods in order)
uv run news-agg ingest --source ada-derana-en --backfill --concurrency 5
```

#### Review (LLM quality check)

```bash
# Sample and review articles
uv run news-agg review --sample 50

# Review + save passing articles to knowledge graph
uv run news-agg review --sample 50 --save

# Filter by source or date
uv run news-agg review --sample 20 --source ada-derana-en --since 2026-01-01
```

#### Search (Meilisearch)

```bash
# Sync articles from PostgreSQL → Meilisearch
uv run news-agg search sync

# Full-text search
uv run news-agg search query "sri lanka economy" --limit 10

# Filter by source, language, category
uv run news-agg search query "election" --source ada-derana-en --lang en --category politics

# Index stats
uv run news-agg search stats
```

#### Snapshots (Cloudflare R2)

Sync data between machines via R2 (S3-compatible, free 10GB):

```bash
# Push all data stores to R2
uv run news-agg snapshot push --all --label my-pc

# Push PostgreSQL only
uv run news-agg snapshot push --label daily

# Push Neo4j only
uv run news-agg snapshot push --neo4j-only

# Pull everything on another PC (PG + Neo4j + rebuild Meilisearch)
uv run news-agg snapshot pull --all

# Pull without rebuilding Meilisearch
uv run news-agg snapshot pull --all --no-search

# List available snapshots
uv run news-agg snapshot list
```

#### Database operations

```bash
# Check article counts per source
uv run news-agg check
uv run news-agg check --supabase

# Bidirectional sync local ↔ Supabase
uv run news-agg sync

# Backup Supabase → local
uv run news-agg backup

# Apply schema migrations
uv run news-agg db-migrate
```

## Multi-source scheduling

When running without `--source`, the pipeline uses an intelligent queue that interleaves scraping across all active sources. Workers pull from whichever source's rate limit has cooled down.

- Per-source rate limiting (configurable in `sources.yaml`)
- Per-source concurrency caps (lower for Cloudflare sites)
- Autoscaling: starts at `--concurrency N`, scales up to 25 based on queue depth, backs off on errors
- Source priorities (lower number = scraped first)

```bash
uv run news-agg ingest --limit 20 --concurrency 5
```

## VPN mode

Route all browser traffic through ProtonVPN (for Cloudflare-heavy sources):

```bash
docker compose --profile vpn up -d
```

This starts Gluetun (VPN gateway) + Playwright routed through the VPN tunnel.

## Running on another PC

```bash
# 1. Clone repo and install
git clone <repo> && cd news-agg/backend && uv sync

# 2. Copy .env (same credentials)
# 3. Start Docker services
docker compose up -d

# 4. Pull all data from R2
uv run news-agg snapshot pull --all

# 5. Run the autonomous pipeline
uv run news-agg agent run
```

## Data stores

| Store | Role | Snapshotted? |
|-------|------|-------------|
| **PostgreSQL** | Source of truth — articles, sources, dead links, agent runs | Yes → R2 |
| **Neo4j** | Knowledge graph (Graphiti entities/relationships) | Yes → R2 |
| **Meilisearch** | Full-text search index (derived from PG) | No — rebuilt on pull |
| **Langfuse** | LLM observability traces (cloud-hosted) | No — cloud service |
| **Supabase** | Optional remote PostgreSQL (for remote ingestion) | N/A — syncs with local |

## Running tests

```bash
cd backend
uv run pytest tests/ -v
```

## Project structure

```
backend/
  src/news_agg/
    cli.py             # Click CLI (ingest, review, search, agent, snapshot)
    config.py           # Pydantic settings (env vars)
    db.py               # asyncpg database operations
    models.py           # Pydantic models (Article, QA fields)
    pipeline.py         # Main ingestion pipeline
    backfill.py         # Archive, NID sweep, date sweep
    scheduler.py        # Multi-source queue with autoscaling
    snapshot.py         # R2 snapshot push/pull (PG + Neo4j)
    search.py           # Meilisearch sync and search
    sources.yaml        # Per-source scraping config
    source_config.py    # YAML config loader
    agents/
      graph.py          # LangGraph agent (create_react_agent)
      tools.py          # Agent tools (ingest, review, search, graph, status)
      runner.py         # LLM review chain (QA + categorization)
      knowledge.py      # Graphiti integration (Neo4j)
      chains.py         # LangChain prompt chains
      state.py          # PipelineState TypedDict
      tracing.py        # Langfuse tracing setup
      prompts/
        orchestrator_v1.yaml  # Agent system prompt
    scraper/
      article.py        # Article page scraper (Playwright)
      browser.py        # Playwright connection + context creation
      listing.py        # Listing page link extraction
      rss.py            # RSS feed parser
    text/
      dates.py          # Date extraction (5-level waterfall)
      dedup.py          # Title-based dedup
      normalize.py      # Text normalization
      language.py       # Language detection
    utils/
      logging.py        # Colored logging
      rate_limit.py     # Async rate limiter
docker/
  init.sql             # PostgreSQL schema + seed sources
  migrations/          # Schema migrations
  searxng/             # SearXNG config
docker-compose.yml     # All services (PG, Playwright, Neo4j, Meili, SearXNG, pgAdmin)
```

## Dead link tracking

Failed scrape URLs are tracked in the `dead_links` table with error classification (404, timeout, 500, cloudflare, empty). The scraper skips known-dead URLs on subsequent runs.

Graduated retry schedule: 7 days → 14 days → 30 days → permanent (never retried).

## Adding new sources

See [backend/ADDING_SOURCES.md](backend/ADDING_SOURCES.md) for a step-by-step guide.
