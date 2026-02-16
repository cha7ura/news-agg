"""Meilisearch integration for full-text article search.

Syncs articles from PostgreSQL → Meilisearch index.
Meilisearch is a derived index — always rebuildable from PostgreSQL.
"""

from __future__ import annotations

import meilisearch
from meilisearch.errors import MeilisearchApiError

from news_agg.config import settings
from news_agg.utils.logging import get_logger, GREEN, YELLOW, DIM, RED, BOLD, RESET

log = get_logger()

INDEX_NAME = "articles"

# Fields to index (not the full content — just enough for search + display)
SEARCHABLE_ATTRS = ["title", "content", "author", "summary", "source_name"]
FILTERABLE_ATTRS = ["source_slug", "language", "category", "qa_status", "published_at"]
SORTABLE_ATTRS = ["published_at", "qa_score", "created_at"]


def get_client() -> meilisearch.Client:
    return meilisearch.Client(settings.meilisearch_url, settings.meilisearch_api_key)


def _configure_index(client: meilisearch.Client) -> None:
    """Create index and set searchable/filterable attributes."""
    try:
        client.get_index(INDEX_NAME)
    except MeilisearchApiError:
        client.create_index(INDEX_NAME, {"primaryKey": "id"})
        log.info(f"  {GREEN}✓{RESET} Created index '{INDEX_NAME}'")

    index = client.index(INDEX_NAME)
    index.update_searchable_attributes(SEARCHABLE_ATTRS)
    index.update_filterable_attributes(FILTERABLE_ATTRS)
    index.update_sortable_attributes(SORTABLE_ATTRS)


async def sync_articles(
    source_slug: str | None = None,
    batch_size: int = 500,
) -> dict:
    """Sync articles from PostgreSQL → Meilisearch.

    Upserts by article UUID — safe to run multiple times.
    Returns {indexed, total}.
    """
    from news_agg.db import get_pool

    client = get_client()
    _configure_index(client)
    index = client.index(INDEX_NAME)

    pool = await get_pool()

    # Count total articles to sync
    if source_slug:
        total = await pool.fetchval(
            """
            SELECT COUNT(*) FROM articles a
            JOIN sources s ON s.id = a.source_id
            WHERE s.slug = $1
            """,
            source_slug,
        )
    else:
        total = await pool.fetchval("SELECT COUNT(*) FROM articles")

    log.info(f"{BOLD}Syncing {total} articles → Meilisearch{RESET}")

    offset = 0
    indexed = 0

    while offset < total:
        if source_slug:
            rows = await pool.fetch(
                """
                SELECT a.id, a.title, a.content, a.excerpt, a.author,
                       a.published_at, a.language, a.url, a.image_url,
                       a.qa_status, a.qa_score, a.category, a.summary,
                       a.created_at,
                       s.name as source_name, s.slug as source_slug
                FROM articles a
                JOIN sources s ON s.id = a.source_id
                WHERE s.slug = $1
                ORDER BY a.created_at
                LIMIT $2 OFFSET $3
                """,
                source_slug,
                batch_size,
                offset,
            )
        else:
            rows = await pool.fetch(
                """
                SELECT a.id, a.title, a.content, a.excerpt, a.author,
                       a.published_at, a.language, a.url, a.image_url,
                       a.qa_status, a.qa_score, a.category, a.summary,
                       a.created_at,
                       s.name as source_name, s.slug as source_slug
                FROM articles a
                JOIN sources s ON s.id = a.source_id
                ORDER BY a.created_at
                LIMIT $1 OFFSET $2
                """,
                batch_size,
                offset,
            )

        docs = []
        for r in rows:
            content = r["content"] or ""
            docs.append({
                "id": str(r["id"]),
                "title": r["title"],
                "content": content[:5000],  # Truncate for search index
                "excerpt": r["excerpt"] or content[:300],
                "author": r["author"],
                "published_at": r["published_at"].isoformat() if r["published_at"] else None,
                "language": r["language"],
                "url": r["url"],
                "image_url": r["image_url"],
                "qa_status": r["qa_status"],
                "qa_score": r["qa_score"],
                "category": r["category"],
                "summary": r["summary"],
                "source_name": r["source_name"],
                "source_slug": r["source_slug"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            })

        if docs:
            index.add_documents(docs)
            indexed += len(docs)

        offset += batch_size
        log.info(f"  {GREEN}▸{RESET} {min(offset, total)}/{total}")

    log.info(f"  {GREEN}✓{RESET} Indexed {indexed} articles")
    return {"indexed": indexed, "total": total}


def search_articles(
    query: str,
    limit: int = 20,
    source_slug: str | None = None,
    language: str | None = None,
    category: str | None = None,
) -> dict:
    """Search articles in Meilisearch.

    Returns {hits, query, processing_time_ms, estimated_total_hits}.
    """
    client = get_client()
    index = client.index(INDEX_NAME)

    filters = []
    if source_slug:
        filters.append(f'source_slug = "{source_slug}"')
    if language:
        filters.append(f'language = "{language}"')
    if category:
        filters.append(f'category = "{category}"')

    params = {
        "limit": limit,
        "attributesToRetrieve": [
            "id", "title", "excerpt", "author", "published_at",
            "url", "source_name", "source_slug", "language",
            "category", "qa_status", "qa_score",
        ],
        "attributesToHighlight": ["title", "content"],
        "sort": ["published_at:desc"],
    }
    if filters:
        params["filter"] = " AND ".join(filters)

    result = index.search(query, params)
    return result


def get_index_stats() -> dict:
    """Get Meilisearch index stats."""
    client = get_client()
    try:
        index = client.index(INDEX_NAME)
        stats = index.get_stats()
        return {
            "number_of_documents": stats.number_of_documents,
            "is_indexing": stats.is_indexing,
        }
    except MeilisearchApiError:
        return {"number_of_documents": 0, "is_indexing": False}
