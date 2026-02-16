"""Orchestrator for article QA review pipeline.

Samples articles from the database, runs them through QA review and
categorization chains, and prints a formatted report. Optionally saves
passing articles to the Graphiti knowledge graph.

All LLM calls are traced via Langfuse when configured.
"""

from __future__ import annotations

import asyncio
import json
import time

from news_agg.agents.chains import build_categorize_chain, build_qa_chain
from news_agg.config import settings
from news_agg.agents.models import CategoryResult, QAReport
from news_agg.agents.tracing import get_langfuse_handler
from news_agg.agents.knowledge import add_article_to_graph, close_graphiti_client
from news_agg.db import (
    fetch_random_articles,
    get_pool,
    get_unreviewed_articles,
    close_pool,
    update_article_qa,
)
from news_agg.utils.logging import get_logger, GREEN, YELLOW, RED, BOLD, DIM, RESET

log = get_logger()

# Minimum delay between LLM calls (seconds)
_CALL_DELAY_S = 2.0
# Retry settings for rate-limited or transient errors
_MAX_RETRIES = 3
_RETRY_BASE_S = 2.0  # exponential: 2s, 4s, 8s


def _is_rate_limit_error(exc: Exception) -> bool:
    """Check if an exception is a rate-limit (429) error."""
    msg = str(exc)
    return "429" in msg or "rate limit" in msg.lower()


def _parse_response(response, model_class):
    """Parse LLM response into a Pydantic model, handling both structured and raw output."""
    # If with_structured_output worked, response is already the model
    if isinstance(response, model_class):
        return response

    # Raw text response — try to extract JSON
    text = response.content if hasattr(response, "content") else str(response)

    # Strip markdown code fences if present
    if "```json" in text:
        text = text.split("```json")[1].split("```")[0]
    elif "```" in text:
        text = text.split("```")[1].split("```")[0]

    return model_class.model_validate_json(text.strip())


async def _invoke_with_retry(chain, input_data: dict, config: dict, model_class, label: str):
    """Invoke a chain with exponential backoff retry on rate-limit errors."""
    for attempt in range(_MAX_RETRIES + 1):
        try:
            raw = await chain.ainvoke(input_data, config=config)
            return _parse_response(raw, model_class)
        except Exception as e:
            if _is_rate_limit_error(e) and attempt < _MAX_RETRIES:
                wait = _RETRY_BASE_S * (2 ** attempt)
                log.warning(
                    f"  {YELLOW}↻{RESET} {label} rate-limited, "
                    f"retry {attempt + 1}/{_MAX_RETRIES} in {wait:.0f}s"
                )
                await asyncio.sleep(wait)
                continue
            raise


async def review_article(
    article: dict,
    qa_chain,
    cat_chain=None,
    categorize_only: bool = False,
    invoke_config: dict | None = None,
):
    """Run QA review and optional categorization on a single article."""
    content = article["content"] or ""
    input_data = {
        "source": article["source_slug"],
        "language": article["language"],
        "title": article["title"] or "(no title)",
        "author": article["author"] or "(none)",
        "published_at": str(article["published_at"] or "(unknown)"),
        "content": content[:2000],
    }

    config = invoke_config or {}
    qa_report = None
    cat_result = None

    if not categorize_only:
        try:
            qa_report = await _invoke_with_retry(
                qa_chain, input_data, config, QAReport, "QA review"
            )
        except Exception as e:
            log.error(f"  {RED}✗{RESET} QA review failed: {e}")
            return article, None, None

        await asyncio.sleep(_CALL_DELAY_S)

        # Only categorize if QA passes
        if qa_report.status == "fail":
            return article, qa_report, None

    if cat_chain:
        try:
            cat_result = await _invoke_with_retry(
                cat_chain, input_data, config, CategoryResult, "Categorization"
            )
        except Exception as e:
            log.error(f"  {RED}✗{RESET} Categorization failed: {e}")

        await asyncio.sleep(_CALL_DELAY_S)

    return article, qa_report, cat_result


def _print_report(results: list[tuple[dict, QAReport | None, CategoryResult | None]], graph_count: int = 0):
    """Print a formatted review report to console."""
    passes = warns = fails = errors = 0

    for article, qa, cat in results:
        title = (article["title"] or "(no title)")[:60]
        source = article["source_slug"]

        if qa is None and cat is None:
            errors += 1
            log.info(f"  {RED}✗{RESET} [{source}] {title}... {DIM}(error){RESET}")
            continue

        if qa:
            status_color = {
                "pass": GREEN, "warn": YELLOW, "fail": RED
            }[qa.status]
            status_icon = {
                "pass": "✓", "warn": "⚠", "fail": "✗"
            }[qa.status]

            if qa.status == "pass":
                passes += 1
            elif qa.status == "warn":
                warns += 1
            else:
                fails += 1

            log.info(
                f"  {status_color}{status_icon}{RESET} [{source}] {title}..."
                f" {DIM}score={qa.content_quality_score}/10{RESET}"
            )

            if qa.issues:
                for issue in qa.issues:
                    sev_color = {"low": DIM, "medium": YELLOW, "high": RED}[issue.severity]
                    log.info(
                        f"    {sev_color}→ {issue.type}: {issue.description}{RESET}"
                    )
                    if issue.suggested_fix:
                        log.info(f"      {DIM}fix: {issue.suggested_fix}{RESET}")
        else:
            passes += 1  # categorize-only mode, no QA status

        if cat:
            log.info(
                f"    {DIM}category={cat.category}"
                f"  entities={cat.entities[:3]}"
                f"  location={cat.location}{RESET}"
            )
            log.info(f"    {DIM}summary: {cat.summary[:120]}{RESET}")

    # Summary
    total = len(results)
    log.info("")
    log.info(f"{BOLD}Review Summary{RESET}")
    summary = (
        f"  {GREEN}{passes} pass{RESET}  "
        f"{YELLOW}{warns} warn{RESET}  "
        f"{RED}{fails} fail{RESET}  "
        f"{DIM}{errors} error{RESET}  "
        f"({total} total)"
    )
    if graph_count:
        summary += f"  {GREEN}+{graph_count} to graph{RESET}"
    log.info(summary)


async def run_review(
    sample: int = 10,
    source: str | None = None,
    since: str | None = None,
    prompt_version: str = "v1",
    categorize_only: bool = False,
    save_to_graph: bool = False,
    unreviewed: bool = False,
    managed_pool: bool = False,
) -> dict:
    """Main entry point: sample articles → review → report → optionally save to graph.

    Returns a summary dict: {total, passes, warns, fails, errors, graph_saved}.

    Args:
        managed_pool: If True, caller manages DB pool lifecycle (don't close on exit).
    """
    pool = await get_pool()

    # Initialize Langfuse tracing (returns None if not configured)
    langfuse_handler = get_langfuse_handler()
    invoke_config = {"callbacks": [langfuse_handler]} if langfuse_handler else {}

    try:
        # Fetch articles: either unreviewed (for agent) or random sample (CLI)
        if unreviewed:
            articles = await get_unreviewed_articles(pool, sample, source)
        else:
            articles = await fetch_random_articles(pool, sample, source, since)
        if not articles:
            log.warning(f"{YELLOW}No articles found matching filters{RESET}")
            return {"total": 0, "passes": 0, "warns": 0, "fails": 0, "errors": 0, "graph_saved": 0}

        log.info(f"{BOLD}REVIEW{RESET} — {len(articles)} articles (prompt={prompt_version})")
        if source:
            log.info(f"  {DIM}source filter: {source}{RESET}")
        if since:
            log.info(f"  {DIM}since: {since}{RESET}")
        if save_to_graph:
            log.info(f"  {DIM}saving passing articles to knowledge graph{RESET}")

        # Build chains
        qa_chain = None if categorize_only else build_qa_chain(prompt_version)
        cat_chain = build_categorize_chain(prompt_version)

        # Process sequentially (respecting rate limits)
        results = []
        graph_count = 0
        start = time.monotonic()

        for i, article in enumerate(articles):
            title = (article["title"] or "")[:50]
            log.info(f"  {DIM}[{i+1}/{len(articles)}] Reviewing: {title}...{RESET}")

            result = await review_article(
                article, qa_chain, cat_chain, categorize_only, invoke_config
            )
            results.append(result)

            # Persist QA results to database
            article_data, qa_report, cat_result = result
            if qa_report and article_data.get("id"):
                try:
                    qa_issues_dicts = [
                        {"type": iss.type, "severity": iss.severity, "description": iss.description}
                        for iss in (qa_report.issues or [])
                    ]
                    await update_article_qa(
                        pool,
                        article_data["id"],
                        qa_status=qa_report.status,
                        qa_score=qa_report.content_quality_score,
                        qa_issues=qa_issues_dicts if qa_issues_dicts else None,
                        category=cat_result.category if cat_result else None,
                        entities=cat_result.entities if cat_result else None,
                        location=cat_result.location if cat_result else None,
                        summary=cat_result.summary if cat_result else None,
                        reviewed_by=settings.active_model,
                    )
                except Exception as e:
                    log.error(f"  {RED}✗{RESET} Failed to persist QA result: {e}")

            # Save to knowledge graph if article passed QA and was categorized
            if save_to_graph and cat_result:
                should_save = categorize_only or (qa_report and qa_report.status == "pass")
                if should_save:
                    saved = await add_article_to_graph(article_data, cat_result)
                    if saved:
                        graph_count += 1

        elapsed = time.monotonic() - start
        log.info(f"  {DIM}Completed in {elapsed:.1f}s{RESET}")
        log.info("")

        _print_report(results, graph_count)

        # Build summary for programmatic callers (agent tools)
        passes = warns = fails = errors = 0
        for _, qa, cat in results:
            if qa is None and cat is None:
                errors += 1
            elif qa:
                if qa.status == "pass":
                    passes += 1
                elif qa.status == "warn":
                    warns += 1
                else:
                    fails += 1
            else:
                passes += 1
        return {
            "total": len(results),
            "passes": passes,
            "warns": warns,
            "fails": fails,
            "errors": errors,
            "graph_saved": graph_count,
        }

    finally:
        if not managed_pool:
            await close_pool()
            await close_graphiti_client()
