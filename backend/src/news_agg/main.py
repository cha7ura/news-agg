"""FastAPI application — REST API for news aggregation pipeline.

Endpoints:
    GET  /health     — Health check (DB connectivity)
    POST /ingest     — Trigger ingestion for one or all sources
    GET  /articles   — List articles with optional source filter
    GET  /stats      — Article counts per source
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Query

from news_agg.db import close_pool, get_article_stats, get_articles, get_pool


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
