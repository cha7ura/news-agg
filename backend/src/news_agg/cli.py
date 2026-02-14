"""Click CLI entry point.

Usage:
    news-agg ingest --source ada-derana-en --limit 20
    news-agg ingest --source ada-derana-si --limit 20 --concurrency 3
    news-agg ingest --source ada-derana-en --backfill --pages 10 --concurrency 5
    news-agg ingest --source ada-derana-en --nid-sweep --concurrency 5
    news-agg check
"""

from __future__ import annotations

import asyncio

import click

from news_agg.utils.logging import GREEN, BOLD, DIM, RESET, get_logger

log = get_logger()


@click.group()
def cli() -> None:
    """News aggregation pipeline CLI."""
    pass


@cli.command()
@click.option("--source", default=None, help="Source slug (e.g., ada-derana-en)")
@click.option("--limit", default=20, help="Max articles per source")
@click.option("--concurrency", default=1, help="Concurrent browser pages (default 1, use 3-5 for backfill)")
@click.option("--backfill", is_flag=True, help="Crawl archive pages for older articles")
@click.option("--pages", default=5, help="Number of archive pages to crawl (backfill only)")
@click.option("--nid-sweep", is_flag=True, help="Sweep through sequential article IDs for full coverage")
@click.option("--date-sweep", is_flag=True, help="Sweep through calendar dates for date-based archive pages")
@click.option("--days", default=None, type=int, help="Limit date sweep to last N days (default: full range)")
def ingest(source: str | None, limit: int, concurrency: int, backfill: bool, pages: int, nid_sweep: bool, date_sweep: bool, days: int | None) -> None:
    """Ingest articles from news sources."""
    asyncio.run(_ingest(source, limit, concurrency, backfill, pages, nid_sweep, date_sweep, days))


async def _ingest(
    source_slug: str | None,
    limit: int,
    concurrency: int,
    backfill: bool,
    pages: int,
    nid_sweep: bool = False,
    date_sweep: bool = False,
    days: int | None = None,
) -> None:
    from news_agg.db import close_pool

    try:
        if date_sweep:
            from news_agg.backfill import run_date_sweep

            result = await run_date_sweep(
                source_slug=source_slug,
                concurrency=concurrency,
                days=days,
            )
        elif nid_sweep:
            from news_agg.backfill import run_nid_sweep

            result = await run_nid_sweep(
                source_slug=source_slug,
                concurrency=concurrency,
            )
        elif backfill:
            from news_agg.backfill import run_backfill

            result = await run_backfill(
                source_slug=source_slug,
                pages=pages,
                concurrency=concurrency,
            )
        else:
            from news_agg.pipeline import run_ingest

            result = await run_ingest(
                source_slug=source_slug,
                limit=limit,
                concurrency=concurrency,
            )
        if "error" in result:
            click.echo(f"Error: {result['error']}")
    finally:
        await close_pool()


@cli.command()
def check() -> None:
    """Show DB stats per source."""
    asyncio.run(_check())


async def _check() -> None:
    from news_agg.db import close_pool, get_article_stats, get_pool

    try:
        pool = await get_pool()
        stats = await get_article_stats(pool)

        click.echo(f"\n{BOLD}News Aggregator — Database Stats{RESET}\n")
        click.echo(f"  {'Source':<30} {'Lang':>4} {'Articles':>8}  {'Latest Article'}")
        click.echo(f"  {'─' * 30} {'─' * 4} {'─' * 8}  {'─' * 20}")

        for row in stats:
            latest = row["latest_article"]
            latest_str = latest.strftime("%Y-%m-%d %H:%M") if latest else "—"
            click.echo(
                f"  {row['name']:<30} {row['language']:>4} {row['count']:>8}  {latest_str}"
            )

        total = sum(r["count"] for r in stats)
        click.echo(f"\n  {GREEN}Total: {total} articles{RESET}\n")
    finally:
        await close_pool()


if __name__ == "__main__":
    cli()
