"""Story clustering pipeline — groups articles about the same news event.

Uses sentence-transformers for embedding generation and cosine similarity
for greedy incremental clustering. No pgvector needed — clustering happens
in Python memory, only story assignments are persisted.

Algorithm:
    1. Fetch recent articles without a story_id
    2. Generate embeddings for "title + first 200 chars"
    3. Greedy incremental clustering: for each article, find the best
       matching cluster (by cosine similarity to centroid). If above
       threshold, join that cluster; otherwise start a new one.
    4. Check each new cluster against existing DB stories (by title similarity)
    5. Create new stories or assign to existing ones
    6. Update story metadata (title, counts, category, etc.)
"""

from __future__ import annotations

import asyncio
import time
from uuid import UUID

import numpy as np

from news_agg.db import get_pool, close_pool
from news_agg.utils.logging import get_logger, GREEN, DIM, BOLD, RESET

log = get_logger()

# Lazy-loaded model — avoids import-time overhead
_model = None

DEFAULT_MODEL = "all-MiniLM-L6-v2"
DEFAULT_THRESHOLD = 0.72
DEFAULT_HOURS = 48


def _get_model():
    """Lazy-load the sentence-transformers model."""
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer

        log.info(f"  {DIM}Loading embedding model: {DEFAULT_MODEL}{RESET}")
        _model = SentenceTransformer(DEFAULT_MODEL)
    return _model


def _make_text(article: dict) -> str:
    """Build embedding input text from article fields."""
    title = article.get("title") or ""
    content = article.get("content") or ""
    # Title is most important; add first 200 chars of content for context
    return f"{title}. {content[:200]}"


def _cosine_similarity(a: np.ndarray, b: np.ndarray) -> float:
    """Cosine similarity between two normalized vectors."""
    return float(np.dot(a, b))


def _generate_embeddings(texts: list[str]) -> np.ndarray:
    """Generate normalized embeddings for a list of texts."""
    model = _get_model()
    return model.encode(texts, normalize_embeddings=True, show_progress_bar=False)


def _cluster_articles(
    embeddings: np.ndarray,
    threshold: float,
) -> list[list[int]]:
    """Greedy incremental clustering. Returns list of clusters (each is a list of article indices).

    For each article, compute cosine similarity to each existing cluster centroid.
    If best match >= threshold, add to that cluster and update centroid.
    Otherwise, start a new cluster.
    """
    clusters: list[tuple[list[int], np.ndarray]] = []  # (indices, centroid)

    for i in range(len(embeddings)):
        best_cluster = -1
        best_sim = 0.0

        for j, (indices, centroid) in enumerate(clusters):
            sim = _cosine_similarity(embeddings[i], centroid)
            if sim > best_sim:
                best_sim = sim
                best_cluster = j

        if best_cluster >= 0 and best_sim >= threshold:
            # Add to existing cluster and update centroid (running average)
            indices, centroid = clusters[best_cluster]
            indices.append(i)
            n = len(indices)
            new_centroid = (centroid * (n - 1) + embeddings[i]) / n
            # Re-normalize
            new_centroid = new_centroid / np.linalg.norm(new_centroid)
            clusters[best_cluster] = (indices, new_centroid)
        else:
            # New cluster
            clusters.append(([i], embeddings[i].copy()))

    return [indices for indices, _ in clusters]


async def _get_existing_story_titles(pool, hours: int) -> list[tuple[UUID, str]]:
    """Fetch recent story titles for matching against new clusters."""
    rows = await pool.fetch(
        """
        SELECT s.id, s.title
        FROM stories s
        WHERE s.last_updated_at >= NOW() - ($1 || ' hours')::interval
        ORDER BY s.last_updated_at DESC
        """,
        str(hours * 2),  # look back further to catch ongoing stories
    )
    return [(r["id"], r["title"]) for r in rows]


async def _find_matching_story(
    cluster_title: str,
    existing_stories: list[tuple[UUID, str]],
    existing_embeddings: np.ndarray | None,
    threshold: float,
) -> UUID | None:
    """Check if a cluster title matches an existing story."""
    if not existing_stories or existing_embeddings is None:
        return None

    cluster_emb = _generate_embeddings([cluster_title])[0]

    best_id = None
    best_sim = 0.0
    for i, (story_id, _) in enumerate(existing_stories):
        sim = _cosine_similarity(cluster_emb, existing_embeddings[i])
        if sim > best_sim:
            best_sim = sim
            best_id = story_id

    if best_sim >= threshold:
        return best_id
    return None


def _pick_best_article(articles: list[dict]) -> dict:
    """Pick the best article from a cluster to represent the story.

    Prefers: highest QA score, then most content, then earliest published.
    """
    return max(
        articles,
        key=lambda a: (
            a.get("qa_score") or 0,
            len(a.get("content") or ""),
            -(a.get("published_at").timestamp() if a.get("published_at") else float("inf")),
        ),
    )


async def _create_story(pool, articles: list[dict]) -> UUID:
    """Create a new story from a cluster of articles."""
    best = _pick_best_article(articles)
    source_slugs = list({a["source_slug"] for a in articles})

    # Use the best article's metadata for the story
    first_published = min(
        (a["published_at"] for a in articles if a.get("published_at")),
        default=None,
    )
    last_updated = max(
        (a["published_at"] for a in articles if a.get("published_at")),
        default=None,
    )

    row = await pool.fetchrow(
        """
        INSERT INTO stories (
            title, summary, category, entities, location, image_url,
            article_count, source_count, first_published_at, last_updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        RETURNING id
        """,
        best.get("title"),
        best.get("summary"),
        best.get("category"),
        best.get("entities"),
        best.get("location"),
        best.get("image_url"),
        len(articles),
        len(source_slugs),
        first_published,
        last_updated,
    )
    return row["id"]


async def _assign_articles_to_story(pool, story_id: UUID, article_ids: list[UUID]) -> None:
    """Set story_id on articles and update story metadata."""
    await pool.execute(
        "UPDATE articles SET story_id = $1 WHERE id = ANY($2::uuid[])",
        story_id,
        article_ids,
    )


async def _update_story_metadata(pool, story_id: UUID) -> None:
    """Recalculate story metadata from its articles."""
    await pool.execute(
        """
        UPDATE stories SET
            article_count = sub.article_count,
            source_count = sub.source_count,
            first_published_at = sub.first_pub,
            last_updated_at = sub.last_pub,
            updated_at = NOW()
        FROM (
            SELECT
                a.story_id,
                COUNT(*) as article_count,
                COUNT(DISTINCT s.slug) as source_count,
                MIN(a.published_at) as first_pub,
                MAX(a.published_at) as last_pub
            FROM articles a
            JOIN sources s ON s.id = a.source_id
            WHERE a.story_id = $1
            GROUP BY a.story_id
        ) sub
        WHERE stories.id = $1
        """,
        story_id,
    )


async def cluster_recent_articles(
    hours: int = DEFAULT_HOURS,
    threshold: float = DEFAULT_THRESHOLD,
    managed_pool: bool = False,
) -> dict:
    """Main entry point: cluster recent articles into stories.

    Returns: {articles_processed, stories_created, articles_assigned, stories_updated}
    """
    pool = await get_pool()
    start = time.monotonic()

    try:
        # 1. Fetch articles without a story_id
        articles = await pool.fetch(
            """
            SELECT a.id, a.title, a.content, a.published_at, a.image_url,
                   a.category, a.entities, a.location, a.summary, a.qa_score,
                   s.slug as source_slug, s.name as source_name
            FROM articles a
            JOIN sources s ON s.id = a.source_id
            WHERE a.story_id IS NULL
              AND a.created_at >= NOW() - ($1 || ' hours')::interval
              AND a.title IS NOT NULL
            ORDER BY a.published_at DESC NULLS LAST
            """,
            str(hours),
        )
        articles = [dict(a) for a in articles]

        if not articles:
            log.info(f"  {DIM}No unclustered articles in last {hours}h{RESET}")
            return {
                "articles_processed": 0,
                "stories_created": 0,
                "articles_assigned": 0,
                "stories_updated": 0,
            }

        log.info(f"{BOLD}CLUSTER{RESET} — {len(articles)} articles from last {hours}h (threshold={threshold})")

        # 2. Generate embeddings
        texts = [_make_text(a) for a in articles]
        embeddings = _generate_embeddings(texts)
        log.info(f"  {DIM}Generated {len(embeddings)} embeddings ({embeddings.shape[1]}d){RESET}")

        # 3. Greedy clustering
        clusters = _cluster_articles(embeddings, threshold)
        multi_article_clusters = [c for c in clusters if len(c) > 1]
        singletons = [c for c in clusters if len(c) == 1]
        log.info(
            f"  {DIM}Found {len(multi_article_clusters)} multi-source clusters, "
            f"{len(singletons)} singletons{RESET}"
        )

        # 4. Get existing stories for matching
        existing_stories = await _get_existing_story_titles(pool, hours)
        existing_embeddings = None
        if existing_stories:
            existing_titles = [title for _, title in existing_stories]
            existing_embeddings = _generate_embeddings(existing_titles)
            log.info(f"  {DIM}Comparing against {len(existing_stories)} existing stories{RESET}")

        # 5. Process each cluster
        stories_created = 0
        stories_updated = 0
        articles_assigned = 0

        for cluster_indices in clusters:
            cluster_articles = [articles[i] for i in cluster_indices]
            cluster_ids = [a["id"] for a in cluster_articles]
            best = _pick_best_article(cluster_articles)

            # Try to match to existing story
            matched_story_id = await _find_matching_story(
                best["title"],
                existing_stories,
                existing_embeddings,
                threshold,
            )

            if matched_story_id:
                # Add to existing story
                await _assign_articles_to_story(pool, matched_story_id, cluster_ids)
                await _update_story_metadata(pool, matched_story_id)
                stories_updated += 1
                articles_assigned += len(cluster_ids)
                log.info(
                    f"  {GREEN}+{len(cluster_ids)}{RESET} → existing story: "
                    f"{best['title'][:60]}..."
                )
            else:
                # Create new story
                story_id = await _create_story(pool, cluster_articles)
                await _assign_articles_to_story(pool, story_id, cluster_ids)
                stories_created += 1
                articles_assigned += len(cluster_ids)

                # Add to existing stories list so subsequent clusters can match
                existing_stories.append((story_id, best["title"]))
                new_emb = _generate_embeddings([best["title"]])
                if existing_embeddings is not None:
                    existing_embeddings = np.vstack([existing_embeddings, new_emb])
                else:
                    existing_embeddings = new_emb

                source_count = len({a["source_slug"] for a in cluster_articles})
                log.info(
                    f"  {GREEN}★{RESET} new story ({len(cluster_ids)} articles, "
                    f"{source_count} sources): {best['title'][:60]}..."
                )

        elapsed = time.monotonic() - start
        log.info(
            f"\n{BOLD}Clustering complete{RESET} in {elapsed:.1f}s — "
            f"{stories_created} created, {stories_updated} updated, "
            f"{articles_assigned} articles assigned"
        )

        return {
            "articles_processed": len(articles),
            "stories_created": stories_created,
            "articles_assigned": articles_assigned,
            "stories_updated": stories_updated,
        }

    finally:
        if not managed_pool:
            await close_pool()
