"""FastAPI application — REST API for news aggregation pipeline.

Endpoints:
    GET  /health           — Health check (DB connectivity)
    POST /ingest           — Trigger ingestion for one or all sources
    GET  /articles         — List articles with optional source filter
    GET  /stats            — Article counts per source
    GET  /dashboard/stats  — Full pipeline overview for dashboard
    GET  /                 — Dashboard UI
"""

from __future__ import annotations

import asyncio
import logging
import re
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from news_agg.db import (
    close_pool,
    get_article_stats,
    get_articles,
    get_dashboard_stats,
    get_dead_link_stats,
    get_ingestion_activity,
    get_pool,
    get_recent_runs,
    get_review_model_stats,
    get_stories,
    get_story_detail,
    get_today_stories,
)

_STATIC_DIR = Path(__file__).parent.parent.parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize DB pool on startup, close on shutdown."""
    await get_pool()
    yield
    await close_pool()


app = FastAPI(
    title="News Agg API",
    description="Sri Lankan news aggregation pipeline",
    version="0.1.0",
    lifespan=lifespan,
)

# CORS — allow Next.js frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files (dashboard HTML/CSS/JS)
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/health")
async def health():
    """Health check — verifies DB connectivity."""
    try:
        pool = await get_pool()
        result = await pool.fetchval("SELECT 1")
        return {"status": "ok", "db": result == 1}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@app.post("/ingest")
async def trigger_ingest(
    source: str | None = Query(None, description="Source slug (e.g., ada-derana-en)"),
    limit: int = Query(20, description="Max articles per source"),
):
    """Trigger ingestion for one or all sources."""
    from news_agg.pipeline import run_ingest

    result = await run_ingest(source_slug=source, limit=limit)
    return result


@app.get("/articles")
async def list_articles(
    source: str | None = Query(None, description="Filter by source slug"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List articles with optional source filter."""
    pool = await get_pool()
    articles = await get_articles(pool, source_slug=source, limit=limit, offset=offset)
    return {"articles": _serialize(articles), "count": len(articles)}


@app.get("/stats")
async def stats():
    """Article counts per source."""
    pool = await get_pool()
    rows = await get_article_stats(pool)
    return {"sources": _serialize(rows)}


def _serialize_val(val):
    """Recursively convert UUID/datetime values to JSON-safe strings."""
    if hasattr(val, "hex"):
        return str(val)
    if hasattr(val, "isoformat"):
        return val.isoformat()
    if isinstance(val, dict):
        return {k: _serialize_val(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_serialize_val(item) for item in val]
    return val


def _serialize(rows: list[dict]) -> list[dict]:
    """Serialize a list of dicts for JSON (UUIDs, datetimes, nested structures)."""
    return [_serialize_val(row) for row in rows]


# --- Stories ---

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@app.get("/stories")
async def list_stories(
    date: str | None = Query(None, description="Filter by date (YYYY-MM-DD)"),
    category: str | None = Query(None, description="Filter by category"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """List stories with optional filters."""
    if date and not _DATE_RE.match(date):
        return JSONResponse(status_code=400, content={"error": "Invalid date format, use YYYY-MM-DD"})
    pool = await get_pool()
    stories, total = await get_stories(pool, date=date, category=category, limit=limit, offset=offset)
    return {"stories": _serialize(stories), "total": total}


@app.get("/stories/today")
async def today_stories():
    """Get today's stories, ordered by coverage (most sources first)."""
    pool = await get_pool()
    stories = await get_today_stories(pool)
    return {"stories": _serialize(stories)}


@app.get("/stories/{story_id}")
async def story_detail(story_id: str):
    """Get a single story with all its articles."""
    try:
        sid = UUID(story_id)
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid story ID"})

    pool = await get_pool()
    story = await get_story_detail(pool, sid)
    if not story:
        return JSONResponse(status_code=404, content={"error": "Story not found"})
    return _serialize_val(story)


# --- Search ---

@app.get("/search")
async def search(
    q: str = Query(..., description="Search query"),
    source: str | None = Query(None, description="Filter by source slug"),
    language: str | None = Query(None, description="Filter by language (en, si)"),
    category: str | None = Query(None, description="Filter by category"),
    limit: int = Query(20, ge=1, le=100),
):
    """Full-text search via Meilisearch."""
    from news_agg.search import search_articles

    try:
        result = search_articles(
            query=q, limit=limit,
            source_slug=source, language=language, category=category,
        )
        return {
            "articles": result.get("hits", []),
            "count": result.get("estimatedTotalHits", 0),
            "query": q,
            "processing_time_ms": result.get("processingTimeMs", 0),
        }
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={"error": "Search unavailable", "detail": str(e)},
        )


class AskRequest(BaseModel):
    query: str
    session_id: str | None = None


@app.post("/search/ask")
async def search_ask(body: AskRequest):
    """Conversational RAG — ask questions about the news."""
    from news_agg.rag import ask

    result = await ask(query=body.query.strip(), session_id=body.session_id)
    return result


@app.get("/dashboard/stats")
async def dashboard_stats():
    """Full pipeline overview for the dashboard."""
    pool = await get_pool()

    sources, dead_links, activity, models, runs = await asyncio.gather(
        get_dashboard_stats(pool),
        get_dead_link_stats(pool),
        get_ingestion_activity(pool, days=7),
        get_review_model_stats(pool),
        get_recent_runs(pool, limit=10),
    )

    # Meilisearch stats (graceful if unavailable)
    try:
        from news_agg.search import get_index_stats
        meili = get_index_stats()
    except Exception as e:
        logging.getLogger(__name__).debug("Meilisearch unavailable: %s", e)
        meili = {"number_of_documents": 0, "is_indexing": False}

    # Aggregate totals
    total_articles = sum(s["total_articles"] for s in sources)
    total_reviewed = sum(s["reviewed"] for s in sources)
    total_graph = sum(s["graph_saved"] for s in sources)

    return {
        "totals": {
            "articles": total_articles,
            "reviewed": total_reviewed,
            "graph_saved": total_graph,
            "meilisearch_indexed": meili.get("number_of_documents", 0),
            "meilisearch_indexing": meili.get("is_indexing", False),
        },
        "sources": _serialize(sources),
        "dead_links": _serialize(dead_links),
        "activity": _serialize(activity),
        "models": _serialize(models),
        "runs": _serialize(runs),
    }


@app.get("/")
async def root():
    """Serve the dashboard page."""
    dashboard = _STATIC_DIR / "dashboard.html"
    if dashboard.exists():
        return FileResponse(str(dashboard), media_type="text/html")
    return {"message": "News Agg API", "docs": "/docs"}
