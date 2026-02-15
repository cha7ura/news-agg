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


async def get_pool(database_url: str | None = None) -> asyncpg.Pool:
    global _pool
    if _pool is None:
        url = database_url or settings.database_url
        _pool = await asyncpg.create_pool(url, min_size=2, max_size=10)
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


async def get_dead_urls(pool: asyncpg.Pool, source_id: UUID, urls: list[str]) -> set[str]:
    """Batch check: return URLs that should be skipped (not yet due for retry).

    Retry schedule from first_failed_at:
      retry_count 0 → wait 7 days, 1 → 14 days, 2 → 30 days, 3+ → permanent.
    """
    if not urls:
        return set()
    rows = await pool.fetch(
        """
        SELECT url FROM dead_links
        WHERE source_id = $1 AND url = ANY($2::text[])
        AND (
            retry_count >= 3
            OR (retry_count = 0 AND first_failed_at + interval '7 days' > NOW())
            OR (retry_count = 1 AND first_failed_at + interval '14 days' > NOW())
            OR (retry_count = 2 AND first_failed_at + interval '30 days' > NOW())
        )
        """,
        source_id,
        urls,
    )
    return {r["url"] for r in rows}


async def get_all_dead_urls(pool: asyncpg.Pool, source_id: UUID) -> set[str]:
    """Load ALL dead URLs for a source that should be skipped. Used by NID sweep."""
    rows = await pool.fetch(
        """
        SELECT url FROM dead_links
        WHERE source_id = $1
        AND (
            retry_count >= 3
            OR (retry_count = 0 AND first_failed_at + interval '7 days' > NOW())
            OR (retry_count = 1 AND first_failed_at + interval '14 days' > NOW())
            OR (retry_count = 2 AND first_failed_at + interval '30 days' > NOW())
        )
        """,
        source_id,
    )
    return {r["url"] for r in rows}


async def record_dead_link(
    pool: asyncpg.Pool, source_id: UUID, url: str, error_type: str,
) -> None:
    """Insert a new dead link or increment retry_count on re-failure."""
    await pool.execute(
        """
        INSERT INTO dead_links (source_id, url, error_type)
        VALUES ($1, $2, $3)
        ON CONFLICT (url) DO UPDATE SET
            error_type = EXCLUDED.error_type,
            last_checked_at = NOW(),
            retry_count = dead_links.retry_count + 1
        """,
        source_id,
        url,
        error_type,
    )


async def remove_dead_link(pool: asyncpg.Pool, url: str) -> None:
    """Delete a dead link when a retry succeeds."""
    await pool.execute("DELETE FROM dead_links WHERE url = $1", url)


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


async def fetch_random_articles(
    pool: asyncpg.Pool,
    limit: int = 10,
    source_slug: str | None = None,
    since: str | None = None,
) -> list[dict]:
    """Fetch random articles for QA review. Returns dicts with source metadata."""
    conditions: list[str] = []
    params: list = []
    idx = 1

    if source_slug:
        conditions.append(f"s.slug = ${idx}")
        params.append(source_slug)
        idx += 1

    if since:
        conditions.append(f"a.published_at >= ${idx}::timestamptz")
        params.append(since)
        idx += 1

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)

    rows = await pool.fetch(
        f"""
        SELECT a.id, a.title, a.content, a.author, a.published_at,
               a.image_url, a.language, a.url,
               s.name as source_name, s.slug as source_slug
        FROM articles a
        JOIN sources s ON s.id = a.source_id
        {where}
        ORDER BY RANDOM()
        LIMIT ${idx}
        """,
        *params,
    )
    return [dict(r) for r in rows]


async def get_dead_link_stats(pool: asyncpg.Pool) -> list[dict]:
    """Dead link counts per source. For the `check` CLI command."""
    rows = await pool.fetch(
        """
        SELECT s.name, s.slug, s.language,
               COUNT(d.id) as total,
               COUNT(d.id) FILTER (WHERE d.retry_count >= 3) as permanent,
               COUNT(d.id) FILTER (WHERE d.retry_count < 3) as retryable,
               COUNT(d.id) FILTER (WHERE d.error_type = '404') as err_404,
               COUNT(d.id) FILTER (WHERE d.error_type = 'timeout') as err_timeout,
               COUNT(d.id) FILTER (WHERE d.error_type = 'empty') as err_empty,
               COUNT(d.id) FILTER (WHERE d.error_type NOT IN ('404', 'timeout', 'empty')) as err_other
        FROM sources s
        LEFT JOIN dead_links d ON d.source_id = s.id
        GROUP BY s.id, s.name, s.slug, s.language
        HAVING COUNT(d.id) > 0
        ORDER BY COUNT(d.id) DESC
        """
    )
    return [dict(r) for r in rows]
