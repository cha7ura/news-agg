"""Async PostgreSQL operations using asyncpg.

Direct SQL — no ORM, no Supabase SDK. One connection string to migrate anywhere.
Ported from ground-news Supabase queries in pipeline.ts lines 940-1190.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg

from news_agg.config import settings
from news_agg.models import ArticleCreate, Source

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=10)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def get_active_sources(pool: asyncpg.Pool) -> list[Source]:
    """Get all active news sources. (pipeline.ts lines 940-944)"""
    rows = await pool.fetch("SELECT * FROM sources WHERE is_active = true ORDER BY name")
    return [Source(**dict(r)) for r in rows]


async def get_source_by_slug(pool: asyncpg.Pool, slug: str) -> Source | None:
    row = await pool.fetchrow("SELECT * FROM sources WHERE slug = $1", slug)
    return Source(**dict(row)) if row else None


async def get_existing_urls(pool: asyncpg.Pool, source_id: UUID, urls: list[str]) -> set[str]:
    """Check which URLs already exist for this source. (pipeline.ts lines 1036-1040)"""
    if not urls:
        return set()
    rows = await pool.fetch(
        "SELECT url FROM articles WHERE source_id = $1 AND url = ANY($2::text[])",
        source_id,
        urls,
    )
    return {r["url"] for r in rows}


async def get_all_source_urls(pool: asyncpg.Pool, source_id: UUID) -> set[str]:
    """Get ALL article URLs for a source. Used by nid sweep for pre-dedup."""
    rows = await pool.fetch(
        "SELECT url FROM articles WHERE source_id = $1",
        source_id,
    )
    return {r["url"] for r in rows}


async def get_recent_titles(pool: asyncpg.Pool, source_id: UUID, days: int = 7) -> set[str]:
    """Get normalized titles from last N days for dedup. (pipeline.ts lines 1043-1048)"""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    rows = await pool.fetch(
        "SELECT title FROM articles WHERE source_id = $1 AND created_at >= $2",
        source_id,
        cutoff,
    )
    return {r["title"] for r in rows}


async def insert_article(pool: asyncpg.Pool, article: ArticleCreate) -> UUID | None:
    """Insert article, returning id. Returns None if URL already exists.

    Uses ON CONFLICT DO NOTHING — the url UNIQUE constraint is the safety net.
    (pipeline.ts lines 1150-1166)
    """
    row = await pool.fetchrow(
        """
        INSERT INTO articles (
            source_id, url, title, content, excerpt, image_url, author,
            published_at, language, original_language
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (url) DO NOTHING
        RETURNING id
        """,
        article.source_id,
        article.url,
        article.title,
        article.content,
        article.excerpt,
        article.image_url,
        article.author,
        article.published_at,
        article.language,
        article.original_language,
    )
    return row["id"] if row else None


async def get_article_stats(pool: asyncpg.Pool) -> list[dict]:
    """Get article counts per source. For the `check` CLI command."""
    rows = await pool.fetch(
        """
        SELECT s.name, s.slug, s.language, COUNT(a.id) as count,
               MAX(a.published_at) as latest_article
        FROM sources s
        LEFT JOIN articles a ON a.source_id = s.id
        GROUP BY s.id, s.name, s.slug, s.language
        ORDER BY s.name
        """
    )
    return [dict(r) for r in rows]


async def get_articles(
    pool: asyncpg.Pool,
    source_slug: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> list[dict]:
    """Get articles with optional source filter. For the API."""
    if source_slug:
        rows = await pool.fetch(
            """
            SELECT a.*, s.name as source_name, s.slug as source_slug
            FROM articles a
            JOIN sources s ON s.id = a.source_id
            WHERE s.slug = $1
            ORDER BY a.published_at DESC NULLS LAST
            LIMIT $2 OFFSET $3
            """,
            source_slug,
            limit,
            offset,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT a.*, s.name as source_name, s.slug as source_slug
            FROM articles a
            JOIN sources s ON s.id = a.source_id
            ORDER BY a.published_at DESC NULLS LAST
            LIMIT $1 OFFSET $2
            """,
            limit,
            offset,
        )
    return [dict(r) for r in rows]
