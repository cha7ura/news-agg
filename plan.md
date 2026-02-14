# ADR-001: Architecture for news-agg — Sri Lankan News Aggregation Pipeline

**Date**: 2026-02-14
**Status**: Accepted
**Repo**: `news-agg`

---

## Context

We are building a news aggregation and analysis system for Sri Lankan media. The system will:

1. Collect news articles from English and Sinhala news websites
2. Extract entities, events, and relationships from news content
3. Build a temporal knowledge graph tracking stories, politicians, and topics over time
4. Enable cross-document analysis, bias detection, and fact-checking
5. Serve results via API and a web frontend

**Starting scope**: Ada Derana English + Sinhala only. Expand to 30+ sources over time.

---

## Decision: Python + PostgreSQL + Neo4j

### Technology Stack

| Layer | Technology | Rationale |
|-------|-----------|-----------|
| **Language** | Python 3.12+ | Best ML/NLP ecosystem, Graphiti is Python-native |
| **Scraping** | Playwright (Python) | Handles JS-rendered sites, same API as Node version |
| **Article Storage** | PostgreSQL (via asyncpg) | Battle-tested, JSONB, full-text search, pgvector |
| **Knowledge Graph** | Neo4j + Graphiti | Temporal KG with entity resolution, hybrid search |
| **Embeddings** | multilingual-e5-base | Supports English + Sinhala, open-source |
| **LLM (enrichment)** | OpenRouter / Ollama | Entity extraction, summarization, translation |
| **API** | FastAPI | Async Python, auto-docs, high performance |
| **Frontend** | TBD (Phase 5+) | New frontend built in this repo later |
| **Scheduling** | APScheduler / cron | Periodic scraping runs |
| **Containerization** | Docker Compose | PostgreSQL + Neo4j + Playwright + API |

### Database Strategy

**PostgreSQL** is the primary relational store for articles, sources, and metadata.

- **Initial deployment**: Connect to the existing Supabase PostgreSQL instance (same DB the Next.js frontend uses). Immediate visibility of scraped articles on the existing frontend during development.
- **Production deployment**: Self-hosted PostgreSQL (Docker) or managed instance. Code uses `asyncpg` directly — no Supabase SDK — so migration is changing one connection string.
- **Knowledge graph**: Neo4j runs alongside PostgreSQL. Articles live in PostgreSQL; entities, relationships, and temporal facts live in Neo4j via Graphiti.

```
PostgreSQL (articles, sources, metadata)
    ↕ article_id references
Neo4j (entities, relationships, temporal edges via Graphiti)
```

### Why Python Over TypeScript?

A working 1,773-line TypeScript pipeline already exists in the `ground-news` repo. We are rebuilding in Python because:

1. **Graphiti is Python-only** — the temporal knowledge graph framework (Phase 2) has no TypeScript client
2. **ML/NLP ecosystem** — spaCy, HuggingFace, sentence-transformers, LangChain are Python-first
3. **Research alignment** — SinLlama and university collaborations use Python
4. **Self-hosted LLM inference** — llama-cpp-python, vLLM, Ollama Python clients are more mature
5. **Long-term vision** — Phases 2-6 are heavily Python-dependent; rebuilding Phase 1 now avoids a painful migration later

The TypeScript pipeline in `ground-news` remains operational during the transition. Both systems share the same PostgreSQL database.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Ingestion Layer                           │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌───────────────────┐   │
│  │ RSS Parser   │  │  Listing    │  │  Article Page     │   │
│  │ (feedparser) │  │  Scraper    │  │  Scraper          │   │
│  │              │  │ (Playwright)│  │  (Playwright)     │   │
│  └──────┬──────┘  └──────┬──────┘  └────────┬──────────┘   │
│         └────────────────┼──────────────────┘               │
│                          ▼                                   │
│                 ┌────────────────┐                           │
│                 │  Text Pipeline │                           │
│                 │  normalize     │                           │
│                 │  date extract  │                           │
│                 │  dedup         │                           │
│                 │  lang detect   │                           │
│                 └───────┬────────┘                           │
│                         ▼                                    │
│                 ┌────────────────┐                           │
│                 │  PostgreSQL    │                           │
│                 │  (articles)    │                           │
│                 └────────────────┘                           │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                  Enrichment Layer (Phase 2)                  │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌───────────────────┐   │
│  │ Entity       │  │ Translation │  │  Embedding        │   │
│  │ Extraction   │  │ EN↔SI       │  │  Generation       │   │
│  │ (LLM)        │  │ (LLM)      │  │  (e5-base)        │   │
│  └──────┬──────┘  └──────┬──────┘  └────────┬──────────┘   │
│         └────────────────┼──────────────────┘               │
│                          ▼                                   │
│                 ┌────────────────┐                           │
│                 │  Graphiti +    │                           │
│                 │  Neo4j         │                           │
│                 └────────────────┘                           │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                    API + Frontend (Phase 3+)                │
│                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌───────────────────┐   │
│  │ FastAPI      │  │  Frontend   │  │  Scheduler        │   │
│  │ REST API     │  │  (TBD)      │  │  (APScheduler)    │   │
│  └─────────────┘  └─────────────┘  └───────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
news-agg/
├── pyproject.toml
├── docker-compose.yml
├── .env.example
├── README.md
├── docs/
│   └── ADR-001.md
│
├── src/
│   └── news_agg/
│       ├── __init__.py
│       ├── cli.py              # Click CLI entry point
│       ├── config.py           # Env vars, source definitions
│       ├── db.py               # asyncpg PostgreSQL operations
│       ├── models.py           # Pydantic models
│       │
│       ├── scraper/
│       │   ├── __init__.py
│       │   ├── browser.py      # Playwright lifecycle
│       │   ├── rss.py          # RSS feed parser
│       │   ├── listing.py      # Listing page link discovery
│       │   └── article.py      # Article page content extraction
│       │
│       ├── text/
│       │   ├── __init__.py
│       │   ├── normalize.py    # Unicode, HTML entities, mojibake
│       │   ├── dates.py        # Date extraction waterfall
│       │   ├── dedup.py        # Title normalization (Sinhala-safe)
│       │   └── language.py     # Language detection (Unicode ranges)
│       │
│       └── utils/
│           ├── __init__.py
│           ├── rate_limit.py   # Polite scraping delays
│           └── logging.py      # Structured colored logging
│
└── tests/
    ├── conftest.py
    ├── test_dates.py
    ├── test_normalize.py
    ├── test_dedup.py
    ├── test_rss.py
    └── test_article.py
```

---

## Phase 1: Ada Derana Scraping + Storage (Current)

**Goal**: Scrape Ada Derana English + Sinhala articles and store in PostgreSQL.

### Sources

| Source | Slug | Language | Discovery | Notes |
|--------|------|----------|-----------|-------|
| Ada Derana | `ada-derana-en` | English | RSS (`/rss.php`) | Working RSS feed |
| අද දෙරණ | `ada-derana-si` | Sinhala | Listing page | RSS broken (PHP fatal error) |

### Ada Derana Technical Details

- **No og: meta tags, no JSON-LD, no author attribution**
- **EN article URLs**: `/news.php?nid=NNNNN` or `/news/NNNNN` (two formats, same article)
- **SI article URLs**: `/news/NNNNN`
- **EN listing page**: `/hot-news/`
- **SI listing page**: `/` (root — `/hot-news/` returns 404)
- **Date format**: `"Month DD, YYYY HH:MM am/pm"` (e.g., "February 4, 2026 02:39 pm")
- **Content selectors**: `.news-content` → `#news_body` → `article` → `main`
- **Date selectors**: `.news-datestamp` → `time[datetime]`

### Data Flow

```
1. Discover article URLs
   ├── EN: Parse RSS feed (feedparser)
   └── SI: Scrape listing page links (Playwright)

2. Deduplicate
   ├── URL exact match against PostgreSQL
   └── Normalized title match against last 7 days

3. Scrape each new article (Playwright)
   ├── Extract: title, content, date, image_url
   ├── Date waterfall: meta → selector datetime → selector text → URL → body regex → RSS pubDate
   ├── Normalize text (NFC, HTML entities, mojibake)
   └── Skip if: no date OR content < 100 chars

4. Store in PostgreSQL (articles table)
```

### Articles Table Schema

```sql
CREATE TABLE articles (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id       UUID NOT NULL REFERENCES sources(id),
    url             TEXT UNIQUE NOT NULL,
    title           TEXT NOT NULL,
    content         TEXT,
    excerpt         TEXT,
    image_url       TEXT,
    author          TEXT,
    published_at    TIMESTAMPTZ,
    scraped_at      TIMESTAMPTZ DEFAULT NOW(),
    language        TEXT DEFAULT 'en',
    original_language TEXT DEFAULT 'en',
    is_processed    BOOLEAN DEFAULT false,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
```

### Phase 1 Deliverables

- [ ] Python project with `pyproject.toml` and dependencies
- [ ] CLI: `news-agg ingest --source ada-derana-en --limit 20`
- [ ] CLI: `news-agg ingest --source ada-derana-si --limit 20`
- [ ] CLI: `news-agg check` (show DB stats per source)
- [ ] Unit tests for text processing (dates, normalize, dedup)
- [ ] Integration test: scrape 1 article, verify in DB

---

## Phase 2: Enrichment + Knowledge Graph

**Goal**: Extract entities and build temporal knowledge graph.

- LLM-based entity extraction (people, organizations, locations)
- Bilingual prompts (English + Sinhala)
- Entity alias resolution (Sri Lankan politicians, organizations)
- Cross-language translation (EN↔SI summaries)
- Embedding generation (multilingual-e5-base)
- Graphiti temporal knowledge graph construction
- Neo4j graph storage and queries

---

## Phase 3: More Sources + Scheduling

**Goal**: Expand to 30+ sources with automated scheduling.

- Daily Mirror, Hiru News, Lankadeepa, News First, etc.
- Per-source scrape configs (selectors, rate limits, methods)
- APScheduler for periodic runs (every 30 min)
- Cloudflare bypass for protected sources
- WordPress REST API support
- Monitoring and alerting

---

## Phase 4: API

**Goal**: FastAPI REST API for querying articles and knowledge graph.

- Article search (full-text + semantic)
- Temporal queries ("What did X say about Y in January?")
- Entity relationship queries
- Stance comparison over time
- OpenAPI documentation

---

## Phase 5: Frontend

**Goal**: New web frontend for visualization.

- Article browser with bias indicators
- Knowledge graph visualization
- Politician stance timelines
- Incident/crime mapping

---

## Phase 6: Production + Scale

**Goal**: Production deployment with monitoring.

- Docker Compose full stack
- Prometheus metrics + Grafana
- CI/CD pipeline
- Backup and recovery

---

## Key Dependencies (Phase 1)

```toml
[project]
name = "news-agg"
version = "0.1.0"
requires-python = ">=3.12"

dependencies = [
    "playwright>=1.49",
    "asyncpg>=0.30",
    "feedparser>=6.0",
    "click>=8.0",
    "python-dotenv>=1.0",
    "pydantic>=2.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.0",
    "pytest-asyncio>=0.24",
    "ruff>=0.8",
]
```

---

## Design Principles

1. **Date is mandatory** — Articles without a parseable date are skipped
2. **Content minimum** — Articles with < 100 chars of content are rejected
3. **Sinhala text preservation** — ZWJ (U+200D) and ZWNJ (U+200C) must be preserved
4. **Try-finally for browser resources** — Always close Playwright pages/contexts in `finally` blocks
5. **Rate limiting** — Minimum 2-second delay between requests to same domain
6. **URL dedup is the safety net** — `url UNIQUE` constraint prevents duplicates at DB level
7. **Portable PostgreSQL** — Use `asyncpg` directly, not Supabase SDK. One connection string to migrate.
8. **Fail fast, log clearly** — Structured logging with source context. No silent failures.

---

## Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Ada Derana changes site structure | Medium | High | CSS selector fallback chains; monitoring |
| Sinhala entity extraction poor quality | High | Medium | Start English-first; bilingual prompts; SinLlama |
| Playwright container connectivity | Low | High | 15s timeout; clear errors; Docker health checks |
| Supabase connection limits | Low | Medium | Connection pooling; migrate to self-hosted PG later |
| LLM API costs (Phase 2) | Medium | Medium | Self-host Ollama; batch processing; caching |

---

## Success Metrics (Phase 1)

- Scrape success rate > 95% for Ada Derana EN
- Scrape success rate > 90% for Ada Derana SI
- Date extraction accuracy > 95%
- Zero duplicate articles (URL + title dedup)
- Full pipeline run < 5 minutes for 20 articles
