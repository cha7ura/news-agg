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
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

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

    # Convert UUID and datetime to strings for JSON serialization
    for article in articles:
        for key, val in article.items():
            if hasattr(val, "hex"):  # UUID
                article[key] = str(val)
            elif hasattr(val, "isoformat"):  # datetime
                article[key] = val.isoformat()

    return {"articles": articles, "count": len(articles)}


@app.get("/stats")
async def stats():
    """Article counts per source."""
    pool = await get_pool()
    rows = await get_article_stats(pool)

    for row in rows:
        for key, val in row.items():
            if hasattr(val, "hex"):
                row[key] = str(val)
            elif hasattr(val, "isoformat"):
                row[key] = val.isoformat()

    return {"sources": rows}


def _serialize(rows: list[dict]) -> list[dict]:
    """Convert UUID/datetime values to strings for JSON serialization."""
    for row in rows:
        for key, val in row.items():
            if hasattr(val, "hex"):
                row[key] = str(val)
            elif hasattr(val, "isoformat"):
                row[key] = val.isoformat()
    return rows


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
