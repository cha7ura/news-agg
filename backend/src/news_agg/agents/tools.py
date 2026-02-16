"""LangGraph tool definitions for the agentic pipeline.

Each tool wraps existing pipeline/DB functionality and returns
structured results the LLM agent can reason about.
"""

from __future__ import annotations

import json

from langchain_core.tools import tool

from news_agg.config import settings
from news_agg.db import (
    get_article_stats,
    get_graph_ready_articles,
    get_pool,
    get_recent_runs,
    mark_article_graph_saved,
    update_agent_run,
)
from news_agg.utils.logging import get_logger

log = get_logger()


@tool
async def get_pipeline_status() -> str:
    """Get current pipeline status: article counts per source, unreviewed counts, last ingest times.

    Use this to decide which sources need ingestion and how many articles need review.
    """
    pool = await get_pool()
    stats = await get_article_stats(pool)
    total_unreviewed = sum(row["unreviewed"] for row in stats)

    lines = ["Pipeline Status:", f"  Total unreviewed: {total_unreviewed}", ""]
    for row in stats:
        latest = row["latest_article"]
        latest_str = latest.strftime("%Y-%m-%d %H:%M") if latest else "never"
        lines.append(
            f"  {row['name']} ({row['slug']}): "
            f"{row['count']} articles, {row['unreviewed']} unreviewed, "
            f"last={latest_str}, lang={row['language']}"
        )

    return "\n".join(lines)


@tool
async def get_run_history(limit: int = 5) -> str:
    """Get recent agent run history to understand what has been done recently.

    Args:
        limit: Number of recent runs to fetch (default 5).
    """
    pool = await get_pool()
    runs = await get_recent_runs(pool, limit)

    if not runs:
        return "No previous agent runs found."

    lines = ["Recent Agent Runs:"]
    for run in runs:
        started = run["started_at"].strftime("%Y-%m-%d %H:%M") if run["started_at"] else "?"
        status = run["status"]
        run_type = run["run_type"]
        result = run.get("result", {})
        error = run.get("error_message")

        line = f"  [{started}] {run_type} — {status}"
        if result:
            line += f" | result={json.dumps(result)}"
        if error:
            line += f" | error={error}"
        lines.append(line)

    return "\n".join(lines)


@tool
async def ingest_source(source_slug: str, limit: int = 20, concurrency: int = 1) -> str:
    """Ingest new articles from a specific news source.

    Discovers article URLs (via RSS or listing page), deduplicates against
    the database, scrapes new articles with Playwright, and inserts them.

    Args:
        source_slug: The source identifier (e.g. 'ada-derana-en', 'island-en').
        limit: Maximum articles to ingest (default 20).
        concurrency: Concurrent browser pages (use 1 for Cloudflare sites, 3-5 for RSS).
    """
    from news_agg.pipeline import run_ingest

    try:
        result = await run_ingest(
            source_slug=source_slug,
            limit=limit,
            concurrency=concurrency,
        )
        if "error" in result:
            return f"Ingest failed for {source_slug}: {result['error']}"

        inserted = result.get("inserted", 0)
        skipped_dup = result.get("skipped_duplicate", 0)
        skipped_nodate = result.get("skipped_no_date", 0)
        return (
            f"Ingested {source_slug}: {inserted} new, "
            f"{skipped_dup} duplicates skipped, {skipped_nodate} skipped (no date)"
        )
    except Exception as e:
        return f"Ingest error for {source_slug}: {e}"


@tool
async def review_unprocessed(limit: int = 50, source_slug: str | None = None) -> str:
    """Review unprocessed articles using LLM quality checks and categorization.

    Fetches articles that haven't been QA-reviewed yet, runs them through
    QA scoring and categorization, and persists results to the database.

    Args:
        limit: Maximum articles to review (default 50).
        source_slug: Optional filter to review only one source.
    """
    from news_agg.agents.runner import run_review

    try:
        result = await run_review(
            sample=limit,
            source=source_slug,
            unreviewed=True,
            save_to_graph=False,
        )
        return (
            f"Reviewed {result['total']} articles: "
            f"{result['passes']} pass, {result['warns']} warn, "
            f"{result['fails']} fail, {result['errors']} errors"
        )
    except Exception as e:
        return f"Review error: {e}"


@tool
async def web_search(query: str, max_results: int = 5) -> str:
    """Search the web using SearXNG for background context on news topics.

    Use this to hydrate articles with additional context, verify facts,
    or research entities mentioned in news stories.

    Args:
        query: Search query string.
        max_results: Maximum results to return (default 5).
    """
    try:
        from langchain_community.utilities import SearxSearchWrapper

        search = SearxSearchWrapper(
            searx_host=settings.searxng_url,
            k=max_results,
        )
        results = await search.aresults(query, num_results=max_results)

        if not results:
            return f"No search results for: {query}"

        lines = [f"Search results for '{query}':"]
        for i, r in enumerate(results[:max_results], 1):
            title = r.get("title", "No title")
            snippet = r.get("snippet", "")[:200]
            url = r.get("link", "")
            lines.append(f"  {i}. {title}")
            if snippet:
                lines.append(f"     {snippet}")
            if url:
                lines.append(f"     {url}")

        return "\n".join(lines)
    except Exception as e:
        return f"Search error: {e}"


@tool
async def save_to_graph(limit: int = 20) -> str:
    """Save QA-passed articles to the Neo4j knowledge graph via Graphiti.

    Finds articles where qa_status='pass' and graph_saved=false,
    then adds them as episodes to the knowledge graph.

    Args:
        limit: Maximum articles to save (default 20).
    """
    from news_agg.agents.knowledge import add_article_to_graph, close_graphiti_client
    from news_agg.agents.models import CategoryResult

    pool = await get_pool()

    try:
        articles = await get_graph_ready_articles(pool, limit)
        if not articles:
            return "No articles ready for graph (all QA-passed articles already saved)."

        saved = 0
        failed = 0
        for article in articles:
            # Reconstruct CategoryResult from stored fields
            cat_result = CategoryResult(
                category=article.get("category") or "other",
                entities=article.get("entities") or [],
                location=article.get("location"),
                summary=article.get("summary") or "",
            )
            ok = await add_article_to_graph(article, cat_result)
            if ok:
                await mark_article_graph_saved(pool, article["id"])
                saved += 1
            else:
                failed += 1

        return f"Saved {saved}/{len(articles)} articles to graph ({failed} failed)"
    except Exception as e:
        return f"Graph save error: {e}"
    finally:
        await close_graphiti_client()


@tool
async def save_run_report(
    run_id: str,
    status: str,
    summary: str,
) -> str:
    """Save the final report for an agent run.

    Call this at the end of a pipeline cycle to record what happened.

    Args:
        run_id: The UUID of the current agent run.
        status: Final status ('completed', 'partial', 'failed').
        summary: Human-readable summary of what was accomplished.
    """
    from uuid import UUID

    pool = await get_pool()

    try:
        await update_agent_run(
            pool,
            UUID(run_id),
            status=status,
            result={"summary": summary},
        )
        return f"Run {run_id} marked as {status}"
    except Exception as e:
        return f"Failed to save run report: {e}"


# All tools available to the agent
ALL_TOOLS = [
    get_pipeline_status,
    get_run_history,
    ingest_source,
    review_unprocessed,
    web_search,
    save_to_graph,
    save_run_report,
]
