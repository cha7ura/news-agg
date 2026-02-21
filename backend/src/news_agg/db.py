"""Async PostgreSQL operations using asyncpg.

Direct SQL — no ORM, no Supabase SDK. One connection string to migrate anywhere.
Ported from ground-news Supabase queries in pipeline.ts lines 940-1190.
"""

from __future__ import annotations

from datetime import date as date_type, datetime, timedelta, timezone
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
    """Get article counts per source, including unreviewed count."""
    rows = await pool.fetch(
        """
        SELECT s.name, s.slug, s.language, COUNT(a.id) as count,
               MAX(a.published_at) as latest_article,
               COUNT(a.id) FILTER (WHERE a.qa_status IS NULL) as unreviewed
        FROM sources s
        LEFT JOIN articles a ON a.source_id = s.id
        GROUP BY s.id, s.name, s.slug, s.language
        ORDER BY s.name
        """
    )
    return [dict(r) for r in rows]


async def get_monthly_article_counts(
    pool: asyncpg.Pool,
    months: int = 6,
) -> list[dict]:
    """Monthly article counts per source for the last N months."""
    rows = await pool.fetch(
        """
        SELECT s.slug, s.name, s.language,
               TO_CHAR(DATE_TRUNC('month', a.published_at), 'YYYY-MM') as month,
               COUNT(*) as count
        FROM articles a
        JOIN sources s ON s.id = a.source_id
        WHERE a.published_at >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '1 month' * ($1 - 1)
        GROUP BY s.slug, s.name, s.language, DATE_TRUNC('month', a.published_at)
        ORDER BY month, s.slug
        """,
        months,
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


async def update_article_qa(
    pool: asyncpg.Pool,
    article_id: UUID,
    qa_status: str,
    qa_score: int,
    qa_issues: list[dict] | None = None,
    category: str | None = None,
    entities: list[str] | None = None,
    location: str | None = None,
    summary: str | None = None,
    reviewed_by: str | None = None,
) -> None:
    """Persist QA review results on an article row."""
    import json

    await pool.execute(
        """
        UPDATE articles SET
            qa_status = $2,
            qa_score = $3,
            qa_issues = $4::jsonb,
            category = $5,
            entities = $6,
            location = $7,
            summary = $8,
            reviewed_at = NOW(),
            reviewed_by = $9
        WHERE id = $1
        """,
        article_id,
        qa_status,
        qa_score,
        json.dumps(qa_issues) if qa_issues else None,
        category,
        entities,
        location,
        summary,
        reviewed_by,
    )


async def get_unreviewed_articles(
    pool: asyncpg.Pool,
    limit: int = 50,
    source_slug: str | None = None,
) -> list[dict]:
    """Fetch articles that haven't been QA reviewed yet."""
    if source_slug:
        rows = await pool.fetch(
            """
            SELECT a.id, a.title, a.content, a.author, a.published_at,
                   a.image_url, a.language, a.url,
                   s.name as source_name, s.slug as source_slug
            FROM articles a
            JOIN sources s ON s.id = a.source_id
            WHERE a.qa_status IS NULL AND s.slug = $1
            ORDER BY a.created_at DESC
            LIMIT $2
            """,
            source_slug,
            limit,
        )
    else:
        rows = await pool.fetch(
            """
            SELECT a.id, a.title, a.content, a.author, a.published_at,
                   a.image_url, a.language, a.url,
                   s.name as source_name, s.slug as source_slug
            FROM articles a
            JOIN sources s ON s.id = a.source_id
            WHERE a.qa_status IS NULL
            ORDER BY a.created_at DESC
            LIMIT $1
            """,
            limit,
        )
    return [dict(r) for r in rows]


async def get_unreviewed_count(
    pool: asyncpg.Pool,
    source_slug: str | None = None,
) -> int:
    """Count articles that haven't been QA reviewed."""
    if source_slug:
        return await pool.fetchval(
            """
            SELECT COUNT(*) FROM articles a
            JOIN sources s ON s.id = a.source_id
            WHERE a.qa_status IS NULL AND s.slug = $1
            """,
            source_slug,
        )
    return await pool.fetchval("SELECT COUNT(*) FROM articles WHERE qa_status IS NULL")


async def get_graph_ready_articles(
    pool: asyncpg.Pool,
    limit: int = 20,
) -> list[dict]:
    """Fetch articles that passed QA but haven't been saved to graph yet."""
    rows = await pool.fetch(
        """
        SELECT a.id, a.title, a.content, a.author, a.published_at,
               a.image_url, a.language, a.url, a.category, a.entities,
               a.location, a.summary,
               s.name as source_name, s.slug as source_slug
        FROM articles a
        JOIN sources s ON s.id = a.source_id
        WHERE a.qa_status = 'pass' AND a.graph_saved = false
        ORDER BY a.reviewed_at DESC
        LIMIT $1
        """,
        limit,
    )
    return [dict(r) for r in rows]


async def mark_article_graph_saved(pool: asyncpg.Pool, article_id: UUID) -> None:
    """Mark an article as saved to the knowledge graph."""
    await pool.execute(
        "UPDATE articles SET graph_saved = true WHERE id = $1",
        article_id,
    )


# --- Agent run history ---

async def create_agent_run(
    pool: asyncpg.Pool,
    run_type: str,
    thread_id: str,
    config: dict | None = None,
) -> UUID:
    """Create a new agent run record, return its ID."""
    import json

    row = await pool.fetchrow(
        """
        INSERT INTO agent_runs (run_type, thread_id, config)
        VALUES ($1, $2, $3::jsonb)
        RETURNING id
        """,
        run_type,
        thread_id,
        json.dumps(config or {}),
    )
    return row["id"]


async def update_agent_run(
    pool: asyncpg.Pool,
    run_id: UUID,
    status: str,
    result: dict | None = None,
    decisions: list[dict] | None = None,
    error_message: str | None = None,
) -> None:
    """Update an agent run with completion status and results."""
    import json

    await pool.execute(
        """
        UPDATE agent_runs SET
            status = $2,
            result = $3::jsonb,
            decisions = $4::jsonb,
            completed_at = NOW(),
            error_message = $5
        WHERE id = $1
        """,
        run_id,
        status,
        json.dumps(result or {}),
        json.dumps(decisions or []),
        error_message,
    )


async def get_recent_runs(
    pool: asyncpg.Pool,
    limit: int = 10,
) -> list[dict]:
    """Get the most recent agent runs."""
    rows = await pool.fetch(
        """
        SELECT id, run_type, status, thread_id, config, result,
               decisions, started_at, completed_at, error_message
        FROM agent_runs
        ORDER BY started_at DESC
        LIMIT $1
        """,
        limit,
    )
    return [dict(r) for r in rows]


async def get_dashboard_stats(pool: asyncpg.Pool) -> list[dict]:
    """Per-source stats for the dashboard: articles, QA breakdown, dead links."""
    rows = await pool.fetch(
        """
        SELECT s.name, s.slug, s.language, s.is_active,
               COUNT(a.id) as total_articles,
               COUNT(a.id) FILTER (WHERE a.qa_status IS NOT NULL) as reviewed,
               COUNT(a.id) FILTER (WHERE a.qa_status = 'pass') as qa_pass,
               COUNT(a.id) FILTER (WHERE a.qa_status = 'warn') as qa_warn,
               COUNT(a.id) FILTER (WHERE a.qa_status = 'fail') as qa_fail,
               COUNT(a.id) FILTER (WHERE a.category IS NOT NULL) as categorized,
               COUNT(a.id) FILTER (WHERE a.graph_saved = true) as graph_saved,
               MAX(a.published_at) as latest_article,
               MAX(a.scraped_at) as latest_scrape,
               (SELECT COUNT(*) FROM dead_links d WHERE d.source_id = s.id) as dead_links
        FROM sources s
        LEFT JOIN articles a ON a.source_id = s.id
        GROUP BY s.id, s.name, s.slug, s.language, s.is_active
        ORDER BY COUNT(a.id) DESC
        """
    )
    return [dict(r) for r in rows]


async def get_ingestion_activity(pool: asyncpg.Pool, days: int = 7) -> list[dict]:
    """Articles ingested per day for the last N days."""
    rows = await pool.fetch(
        """
        SELECT DATE(scraped_at) as date, COUNT(*) as count
        FROM articles
        WHERE scraped_at >= NOW() - ($1 || ' days')::interval
        GROUP BY DATE(scraped_at)
        ORDER BY date
        """,
        str(days),
    )
    return [dict(r) for r in rows]


async def get_review_model_stats(pool: asyncpg.Pool) -> list[dict]:
    """Count of articles reviewed by each model."""
    rows = await pool.fetch(
        """
        SELECT reviewed_by, COUNT(*) as count
        FROM articles
        WHERE reviewed_by IS NOT NULL
        GROUP BY reviewed_by
        ORDER BY count DESC
        """
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


# --- Stories ---

async def _attach_story_sources(pool: asyncpg.Pool, stories: list[dict]) -> None:
    """Batch-fetch source info and attach to each story dict."""
    story_ids = [s["id"] for s in stories]
    if not story_ids:
        return
    source_rows = await pool.fetch(
        """
        SELECT a.story_id, src.name, src.slug
        FROM articles a
        JOIN sources src ON src.id = a.source_id
        WHERE a.story_id = ANY($1::uuid[])
        GROUP BY a.story_id, src.name, src.slug
        """,
        story_ids,
    )
    sources_by_story: dict = {}
    for r in source_rows:
        sources_by_story.setdefault(r["story_id"], []).append(
            {"name": r["name"], "slug": r["slug"]}
        )
    for story in stories:
        story["sources"] = sources_by_story.get(story["id"], [])


async def get_stories(
    pool: asyncpg.Pool,
    date: str | None = None,
    category: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Get stories with optional filters. Returns (stories, total_count)."""
    conditions: list[str] = []
    params: list = []
    idx = 1

    if date:
        conditions.append(f"DATE(s.first_published_at) = ${idx}::date")
        params.append(date)
        idx += 1

    if category:
        conditions.append(f"s.category = ${idx}")
        params.append(category)
        idx += 1

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    # Count
    count = await pool.fetchval(
        f"SELECT COUNT(*) FROM stories s {where}", *params
    )

    # Fetch stories
    params.extend([limit, offset])
    rows = await pool.fetch(
        f"""
        SELECT s.*
        FROM stories s
        {where}
        ORDER BY s.last_updated_at DESC NULLS LAST
        LIMIT ${idx} OFFSET ${idx + 1}
        """,
        *params,
    )
    stories = [dict(r) for r in rows]
    await _attach_story_sources(pool, stories)
    return stories, count


async def get_today_stories(pool: asyncpg.Pool) -> list[dict]:
    """Get today's stories ordered by article_count (most covered first)."""
    rows = await pool.fetch(
        """
        SELECT s.*
        FROM stories s
        WHERE DATE(s.first_published_at) = CURRENT_DATE
           OR DATE(s.last_updated_at) = CURRENT_DATE
        ORDER BY s.article_count DESC, s.last_updated_at DESC
        LIMIT 50
        """
    )
    stories = [dict(r) for r in rows]
    await _attach_story_sources(pool, stories)
    return stories


async def get_story_detail(pool: asyncpg.Pool, story_id: UUID) -> dict | None:
    """Get a single story with all its articles."""
    row = await pool.fetchrow("SELECT * FROM stories WHERE id = $1", story_id)
    if not row:
        return None

    story = dict(row)

    # Fetch all articles in this story
    article_rows = await pool.fetch(
        """
        SELECT a.id, a.title, a.content, a.excerpt, a.url, a.author,
               a.published_at, a.image_url, a.language, a.category,
               a.entities, a.location, a.summary, a.qa_score,
               s.name as source_name, s.slug as source_slug
        FROM articles a
        JOIN sources s ON s.id = a.source_id
        WHERE a.story_id = $1
        ORDER BY a.qa_score DESC NULLS LAST, a.published_at ASC
        """,
        story_id,
    )
    story["articles"] = [dict(r) for r in article_rows]

    # Source list (sorted deterministically by name)
    story["sources"] = sorted(
        [{"name": n, "slug": s} for n, s in {
            (a["source_name"], a["source_slug"]) for a in story["articles"]
        }],
        key=lambda x: x["name"],
    )

    return story


# --- Coverage Audit ---

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
    # asyncpg needs date objects, not strings
    since_date = date_type.fromisoformat(since) if isinstance(since, str) else since
    until_date = date_type.fromisoformat(until) if isinstance(until, str) else until

    conditions = ["TRUE"]
    params: list = [since_date, until_date]
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
