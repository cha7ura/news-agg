"""Click CLI entry point.

Usage:
    news-agg ingest --source ada-derana-en --limit 20
    news-agg ingest --source ada-derana-si --limit 20 --concurrency 3
    news-agg ingest --source ada-derana-en --backfill --pages 10 --concurrency 5
    news-agg ingest --source ada-derana-en --nid-sweep --concurrency 5
    news-agg ingest --source newsfirst-en --limit 5 --supabase
    news-agg check --supabase
    news-agg migrate
    news-agg backup
    news-agg review --sample 10 --source ft-en
    news-agg run --ingest-interval 300 --review-batch 20 --concurrency 3
"""

from __future__ import annotations

import asyncio
import signal

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
@click.option("--backfill", is_flag=True, help="Auto-run configured backfill methods (archive, nid_sweep, date_sweep)")
@click.option("--pages", default=5, help="Number of archive pages to crawl (backfill only)")
@click.option("--nid-sweep", is_flag=True, help="Sweep through sequential article IDs for full coverage")
@click.option("--date-sweep", is_flag=True, help="Sweep through calendar dates for date-based archive pages")
@click.option("--days", default=None, type=int, help="Limit date sweep to last N days (default: full range)")
@click.option("--supabase", is_flag=True, help="Use Supabase DB instead of local")
def ingest(source: str | None, limit: int, concurrency: int, backfill: bool, pages: int, nid_sweep: bool, date_sweep: bool, days: int | None, supabase: bool) -> None:
    """Ingest articles from news sources."""
    if supabase:
        _use_supabase()
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
            from news_agg.backfill import run_auto_backfill

            result = await run_auto_backfill(
                source_slug=source_slug,
                concurrency=concurrency,
                pages=pages,
                days=days,
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


@cli.command("run")
@click.option("--source", default=None, help="Source slug (filter for both pipelines)")
@click.option("--limit", default=20, help="Max articles per source per ingest cycle")
@click.option("--concurrency", default=3, help="Concurrent browser pages for ingestion")
@click.option("--ingest-interval", default=300, help="Seconds between ingestion cycles (default 5m)")
@click.option("--review-batch", default=20, help="Articles per review batch")
@click.option("--review-interval", default=300, help="Seconds between review cycles (default 5m)")
@click.option("--no-review", is_flag=True, help="Skip the review pipeline (ingest only)")
@click.option("--no-ingest", is_flag=True, help="Skip the ingest pipeline (review/sync only)")
@click.option("--no-search-sync", is_flag=True, help="Skip Meilisearch sync after review")
@click.option("--supabase", is_flag=True, help="Use Supabase DB instead of local")
def run_pipelines(
    source: str | None,
    limit: int,
    concurrency: int,
    ingest_interval: int,
    review_batch: int,
    review_interval: int,
    no_review: bool,
    no_ingest: bool,
    no_search_sync: bool,
    supabase: bool,
) -> None:
    """Run dual pipelines: ingestion + processing (review, search sync) concurrently."""
    if supabase:
        _use_supabase()
    if no_review and no_ingest:
        click.echo("Error: Cannot disable both pipelines")
        raise SystemExit(1)
    asyncio.run(_run_dual_pipeline(
        source=source,
        limit=limit,
        concurrency=concurrency,
        ingest_interval=ingest_interval,
        review_batch=review_batch,
        review_interval=review_interval,
        run_ingest_pipeline=not no_ingest,
        run_review_pipeline=not no_review,
        sync_search=not no_search_sync,
    ))


async def _run_dual_pipeline(
    source: str | None,
    limit: int,
    concurrency: int,
    ingest_interval: int,
    review_batch: int,
    review_interval: int,
    run_ingest_pipeline: bool,
    run_review_pipeline: bool,
    sync_search: bool,
) -> None:
    """Run ingestion and processing as concurrent async loops.

    Pipeline 1 (Ingest): discover → scrape → insert articles
    Pipeline 2 (Process): review unreviewed → sync to Meilisearch
    """
    from news_agg.db import close_pool

    log.info(f"{BOLD}DUAL PIPELINE{RESET} — starting concurrent loops")
    if run_ingest_pipeline:
        log.info(f"  {DIM}Ingest: every {ingest_interval}s, limit={limit}, concurrency={concurrency}{RESET}")
    if run_review_pipeline:
        log.info(f"  {DIM}Process: every {review_interval}s, batch={review_batch}, search_sync={sync_search}{RESET}")
    if source:
        log.info(f"  {DIM}Source filter: {source}{RESET}")
    log.info(f"  {DIM}Press Ctrl+C to stop{RESET}\n")

    tasks: list[asyncio.Task] = []

    if run_ingest_pipeline:
        tasks.append(asyncio.create_task(
            _ingest_loop(source, limit, concurrency, ingest_interval),
            name="ingest-loop",
        ))

    if run_review_pipeline:
        tasks.append(asyncio.create_task(
            _process_loop(source, review_batch, review_interval, sync_search),
            name="process-loop",
        ))

    # Register SIGINT/SIGTERM to cancel tasks gracefully
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: [t.cancel() for t in tasks])

    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        log.info(f"\n{BOLD}Shutting down pipelines...{RESET}")
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await close_pool()
        log.info(f"{GREEN}✓{RESET} Pipelines stopped")


async def _ingest_loop(
    source: str | None,
    limit: int,
    concurrency: int,
    interval: int,
) -> None:
    """Continuously ingest articles on an interval."""
    from news_agg.pipeline import run_ingest

    cycle = 0
    while True:
        cycle += 1
        log.info(f"{BOLD}[INGEST #{cycle}]{RESET} starting cycle")
        try:
            result = await run_ingest(
                source_slug=source,
                limit=limit,
                concurrency=concurrency,
            )
            inserted = result.get("inserted", 0)
            if inserted:
                log.info(f"{GREEN}[INGEST #{cycle}]{RESET} +{inserted} articles")
            else:
                log.info(f"{DIM}[INGEST #{cycle}] no new articles{RESET}")
        except Exception as e:
            log.error(f"{RED}[INGEST #{cycle}] error: {e}{RESET}")

        log.info(f"{DIM}[INGEST] next cycle in {interval}s{RESET}")
        await asyncio.sleep(interval)


async def _process_loop(
    source: str | None,
    review_batch: int,
    interval: int,
    sync_search: bool,
) -> None:
    """Continuously review unreviewed articles and sync to Meilisearch."""
    from news_agg.agents.runner import run_review
    from news_agg.search import sync_articles

    cycle = 0
    while True:
        cycle += 1
        log.info(f"{BOLD}[PROCESS #{cycle}]{RESET} starting cycle")

        # Step 1: Review unreviewed articles
        try:
            result = await run_review(
                sample=review_batch,
                source=source,
                unreviewed=True,
                managed_pool=True,
            )
            reviewed = result.get("total", 0)
            passes = result.get("passes", 0)
            if reviewed:
                log.info(
                    f"{GREEN}[PROCESS #{cycle}]{RESET} reviewed {reviewed} "
                    f"({passes} pass, {result.get('warns', 0)} warn, {result.get('fails', 0)} fail)"
                )
            else:
                log.info(f"{DIM}[PROCESS #{cycle}] no unreviewed articles{RESET}")
        except Exception as e:
            log.error(f"{RED}[PROCESS #{cycle}] review error: {e}{RESET}")

        # Step 2: Sync to Meilisearch
        if sync_search:
            try:
                sync_result = await sync_articles(source_slug=source)
                log.info(
                    f"{DIM}[PROCESS #{cycle}] search sync: "
                    f"{sync_result.get('indexed', 0)} indexed{RESET}"
                )
            except Exception as e:
                log.error(f"{RED}[PROCESS #{cycle}] search sync error: {e}{RESET}")

        log.info(f"{DIM}[PROCESS] next cycle in {interval}s{RESET}")
        await asyncio.sleep(interval)


@cli.command()
@click.option("--supabase", is_flag=True, help="Use Supabase DB instead of local")
def check(supabase: bool) -> None:
    """Show DB stats per source."""
    if supabase:
        _use_supabase()
    asyncio.run(_check())


async def _check() -> None:
    from news_agg.db import close_pool, get_article_stats, get_dead_link_stats, get_pool

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

        # Dead link stats
        dead_stats = await get_dead_link_stats(pool)
        if dead_stats:
            click.echo(f"{BOLD}Dead Links{RESET}\n")
            click.echo(f"  {'Source':<30} {'Total':>6} {'Perm':>6} {'Retry':>6}  {'404':>5} {'Tmout':>5} {'Empty':>5} {'Other':>5}")
            click.echo(f"  {'─' * 30} {'─' * 6} {'─' * 6} {'─' * 6}  {'─' * 5} {'─' * 5} {'─' * 5} {'─' * 5}")

            for row in dead_stats:
                click.echo(
                    f"  {row['name']:<30} {row['total']:>6} {row['permanent']:>6} {row['retryable']:>6}"
                    f"  {row['err_404']:>5} {row['err_timeout']:>5} {row['err_empty']:>5} {row['err_other']:>5}"
                )

            dead_total = sum(r["total"] for r in dead_stats)
            click.echo(f"\n  Total: {dead_total} dead links\n")
    finally:
        await close_pool()


def _use_supabase() -> None:
    """Swap database_url to Supabase before any pool creation."""
    from news_agg.config import settings

    if not settings.supabase_database_url:
        click.echo("Error: SUPABASE_DATABASE_URL not set in .env")
        raise SystemExit(1)
    settings.database_url = settings.supabase_database_url
    log.info(f"{BOLD}Using Supabase DB{RESET}")


@cli.command()
def migrate() -> None:
    """Migrate data from local DB to Supabase."""
    asyncio.run(_migrate())


async def _migrate() -> None:
    from pathlib import Path

    import asyncpg

    from news_agg.config import settings

    if not settings.supabase_database_url:
        click.echo("Error: SUPABASE_DATABASE_URL not set in .env")
        return

    # Read schema SQL
    schema_path = Path(__file__).resolve().parents[3] / "docker" / "init.sql"
    if not schema_path.exists():
        click.echo(f"Error: Schema file not found at {schema_path}")
        return
    schema_sql = schema_path.read_text()

    click.echo(f"\n{BOLD}Migrating to Supabase{RESET}\n")

    src_pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=5)
    dst_pool = await asyncpg.create_pool(settings.supabase_database_url, min_size=2, max_size=5)

    try:
        # 1. Apply schema
        click.echo(f"  {DIM}Applying schema...{RESET}")
        await dst_pool.execute(schema_sql)
        click.echo(f"  {GREEN}✓{RESET} Schema applied")

        # 2. Copy sources (delete seed data first so IDs match local DB)
        click.echo(f"  {DIM}Copying sources...{RESET}")
        await dst_pool.execute("DELETE FROM sources WHERE true")
        src_sources = await src_pool.fetch("SELECT * FROM sources ORDER BY name")
        for s in src_sources:
            await dst_pool.execute(
                """
                INSERT INTO sources (id, name, slug, url, rss_url, language, is_active, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (slug) DO NOTHING
                """,
                s["id"], s["name"], s["slug"], s["url"], s["rss_url"],
                s["language"], s["is_active"], s["created_at"], s["updated_at"],
            )
        click.echo(f"  {GREEN}✓{RESET} {len(src_sources)} sources copied")

        # 3. Copy articles in batches using executemany (much faster over network)
        total = await src_pool.fetchval("SELECT COUNT(*) FROM articles")
        dst_before = await dst_pool.fetchval("SELECT COUNT(*) FROM articles")
        click.echo(f"  {DIM}Copying {total} articles ({dst_before} already in target)...{RESET}")

        batch_size = 500
        offset = 0

        insert_sql = """
            INSERT INTO articles (
                source_id, url, title, content, excerpt, image_url, author,
                published_at, scraped_at, language, original_language, is_processed,
                created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            ON CONFLICT (url) DO NOTHING
        """

        while offset < total:
            rows = await src_pool.fetch(
                """
                SELECT source_id, url, title, content, excerpt, image_url, author,
                       published_at, scraped_at, language, original_language, is_processed,
                       created_at, updated_at
                FROM articles
                ORDER BY created_at
                LIMIT $1 OFFSET $2
                """,
                batch_size, offset,
            )

            args = [
                (r["source_id"], r["url"], r["title"], r["content"],
                 r["excerpt"], r["image_url"], r["author"], r["published_at"],
                 r["scraped_at"], r["language"], r["original_language"],
                 r["is_processed"], r["created_at"], r["updated_at"])
                for r in rows
            ]
            await dst_pool.executemany(insert_sql, args)

            offset += batch_size
            click.echo(
                f"  {GREEN}▸{RESET} {min(offset, total)}/{total}"
            )

        dst_after = await dst_pool.fetchval("SELECT COUNT(*) FROM articles")
        inserted = dst_after - dst_before
        click.echo(f"  {GREEN}✓{RESET} {inserted} new articles copied ({dst_after} total in Supabase)")

        # 4. Copy dead_links in batches
        dl_total = await src_pool.fetchval("SELECT COUNT(*) FROM dead_links")
        if dl_total > 0:
            dl_before = await dst_pool.fetchval("SELECT COUNT(*) FROM dead_links")
            click.echo(f"  {DIM}Copying {dl_total} dead links ({dl_before} already in target)...{RESET}")

            dl_insert_sql = """
                INSERT INTO dead_links (
                    source_id, url, error_type, first_failed_at, last_checked_at,
                    retry_count, created_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (url) DO NOTHING
            """
            dl_offset = 0
            while dl_offset < dl_total:
                rows = await src_pool.fetch(
                    """
                    SELECT source_id, url, error_type, first_failed_at, last_checked_at,
                           retry_count, created_at
                    FROM dead_links ORDER BY created_at LIMIT $1 OFFSET $2
                    """,
                    batch_size, dl_offset,
                )
                args = [
                    (r["source_id"], r["url"], r["error_type"], r["first_failed_at"],
                     r["last_checked_at"], r["retry_count"], r["created_at"])
                    for r in rows
                ]
                await dst_pool.executemany(dl_insert_sql, args)
                dl_offset += batch_size

            dl_after = await dst_pool.fetchval("SELECT COUNT(*) FROM dead_links")
            click.echo(f"  {GREEN}✓{RESET} {dl_after - dl_before} new dead links copied")

        click.echo(f"\n  {GREEN}✓{RESET} Migration complete\n")

    finally:
        await src_pool.close()
        await dst_pool.close()


@cli.command()
def backup() -> None:
    """Backup data from Supabase to local DB."""
    asyncio.run(_backup())


async def _backup() -> None:
    from pathlib import Path

    import asyncpg

    from news_agg.config import settings

    if not settings.supabase_database_url:
        click.echo("Error: SUPABASE_DATABASE_URL not set in .env")
        return

    # Read schema SQL
    schema_path = Path(__file__).resolve().parents[3] / "docker" / "init.sql"
    if not schema_path.exists():
        click.echo(f"Error: Schema file not found at {schema_path}")
        return
    schema_sql = schema_path.read_text()

    click.echo(f"\n{BOLD}Backing up from Supabase → Local{RESET}\n")

    src_pool = await asyncpg.create_pool(settings.supabase_database_url, min_size=2, max_size=5)
    dst_pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=5)

    try:
        # 1. Apply schema to local DB
        click.echo(f"  {DIM}Applying schema...{RESET}")
        await dst_pool.execute(schema_sql)
        click.echo(f"  {GREEN}✓{RESET} Schema applied")

        # 2. Sync sources + build ID remapping (Supabase IDs → local IDs)
        click.echo(f"  {DIM}Syncing sources...{RESET}")
        src_sources = await src_pool.fetch("SELECT * FROM sources ORDER BY name")
        for s in src_sources:
            await dst_pool.execute(
                """
                INSERT INTO sources (id, name, slug, url, rss_url, language, is_active, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (slug) DO NOTHING
                """,
                s["id"], s["name"], s["slug"], s["url"], s["rss_url"],
                s["language"], s["is_active"], s["created_at"], s["updated_at"],
            )

        # Build source_id mapping: supabase_id → local_id (via slug)
        supa_sources = {s["id"]: s["slug"] for s in src_sources}
        local_sources = await dst_pool.fetch("SELECT id, slug FROM sources")
        local_by_slug = {s["slug"]: s["id"] for s in local_sources}
        id_map = {}
        for supa_id, slug in supa_sources.items():
            if slug in local_by_slug:
                id_map[supa_id] = local_by_slug[slug]
        click.echo(f"  {GREEN}✓{RESET} {len(src_sources)} sources synced ({len(id_map)} mapped)")

        # 3. Copy articles in batches with source_id remapping
        total = await src_pool.fetchval("SELECT COUNT(*) FROM articles")
        dst_before = await dst_pool.fetchval("SELECT COUNT(*) FROM articles")
        click.echo(f"  {DIM}Copying {total} articles ({dst_before} already in local)...{RESET}")

        batch_size = 500
        offset = 0
        skipped = 0

        insert_sql = """
            INSERT INTO articles (
                source_id, url, title, content, excerpt, image_url, author,
                published_at, scraped_at, language, original_language, is_processed,
                created_at, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
            ON CONFLICT (url) DO NOTHING
        """

        while offset < total:
            rows = await src_pool.fetch(
                """
                SELECT source_id, url, title, content, excerpt, image_url, author,
                       published_at, scraped_at, language, original_language, is_processed,
                       created_at, updated_at
                FROM articles
                ORDER BY created_at
                LIMIT $1 OFFSET $2
                """,
                batch_size, offset,
            )

            args = []
            for r in rows:
                mapped_id = id_map.get(r["source_id"])
                if not mapped_id:
                    skipped += 1
                    continue
                args.append((
                    mapped_id, r["url"], r["title"], r["content"],
                    r["excerpt"], r["image_url"], r["author"], r["published_at"],
                    r["scraped_at"], r["language"], r["original_language"],
                    r["is_processed"], r["created_at"], r["updated_at"],
                ))
            if args:
                await dst_pool.executemany(insert_sql, args)

            offset += batch_size
            click.echo(
                f"  {GREEN}▸{RESET} {min(offset, total)}/{total}"
            )

        dst_after = await dst_pool.fetchval("SELECT COUNT(*) FROM articles")
        inserted = dst_after - dst_before
        click.echo(f"  {GREEN}✓{RESET} {inserted} new articles copied ({dst_after} total in local)")
        if skipped:
            click.echo(f"  {DIM}({skipped} skipped — unmapped source_id){RESET}")

        # 4. Copy dead_links in batches with source_id remapping
        dl_total = await src_pool.fetchval("SELECT COUNT(*) FROM dead_links")
        if dl_total > 0:
            dl_before = await dst_pool.fetchval("SELECT COUNT(*) FROM dead_links")
            click.echo(f"  {DIM}Copying {dl_total} dead links ({dl_before} already in local)...{RESET}")

            dl_insert_sql = """
                INSERT INTO dead_links (
                    source_id, url, error_type, first_failed_at, last_checked_at,
                    retry_count, created_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7)
                ON CONFLICT (url) DO NOTHING
            """
            dl_offset = 0
            while dl_offset < dl_total:
                rows = await src_pool.fetch(
                    """
                    SELECT source_id, url, error_type, first_failed_at, last_checked_at,
                           retry_count, created_at
                    FROM dead_links ORDER BY created_at LIMIT $1 OFFSET $2
                    """,
                    batch_size, dl_offset,
                )
                args = []
                for r in rows:
                    mapped_id = id_map.get(r["source_id"])
                    if not mapped_id:
                        continue
                    args.append((
                        mapped_id, r["url"], r["error_type"], r["first_failed_at"],
                        r["last_checked_at"], r["retry_count"], r["created_at"],
                    ))
                if args:
                    await dst_pool.executemany(dl_insert_sql, args)
                dl_offset += batch_size

            dl_after = await dst_pool.fetchval("SELECT COUNT(*) FROM dead_links")
            click.echo(f"  {GREEN}✓{RESET} {dl_after - dl_before} new dead links copied")

        click.echo(f"\n  {GREEN}✓{RESET} Backup complete\n")

    finally:
        await src_pool.close()
        await dst_pool.close()


@cli.command()
def sync() -> None:
    """Bidirectional sync: push local diff → Supabase, pull Supabase diff → local."""
    asyncio.run(_sync())


async def _sync() -> None:
    import asyncpg

    from news_agg.config import settings

    if not settings.supabase_database_url:
        click.echo("Error: SUPABASE_DATABASE_URL not set in .env")
        return

    click.echo(f"\n{BOLD}Bidirectional Sync — Local ↔ Supabase{RESET}\n")

    local_pool = await asyncpg.create_pool(settings.database_url, min_size=2, max_size=5)
    supa_pool = await asyncpg.create_pool(settings.supabase_database_url, min_size=2, max_size=5)

    batch_size = 500

    article_sql = """
        INSERT INTO articles (
            source_id, url, title, content, excerpt, image_url, author,
            published_at, scraped_at, language, original_language, is_processed,
            created_at, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
        ON CONFLICT (url) DO NOTHING
    """
    article_select = """
        SELECT source_id, url, title, content, excerpt, image_url, author,
               published_at, scraped_at, language, original_language, is_processed,
               created_at, updated_at
        FROM articles ORDER BY created_at LIMIT $1 OFFSET $2
    """
    dl_sql = """
        INSERT INTO dead_links (
            source_id, url, error_type, first_failed_at, last_checked_at,
            retry_count, created_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7)
        ON CONFLICT (url) DO NOTHING
    """
    dl_select = """
        SELECT source_id, url, error_type, first_failed_at, last_checked_at,
               retry_count, created_at
        FROM dead_links ORDER BY created_at LIMIT $1 OFFSET $2
    """

    try:
        # Build source_id mapping (slug-based) between local and Supabase
        local_sources = await local_pool.fetch("SELECT * FROM sources ORDER BY name")
        supa_sources = await supa_pool.fetch("SELECT * FROM sources ORDER BY name")
        local_by_slug = {s["slug"]: s for s in local_sources}
        supa_by_slug = {s["slug"]: s for s in supa_sources}

        # Ensure all local sources exist in Supabase and vice-versa
        for s in local_sources:
            if s["slug"] not in supa_by_slug:
                await supa_pool.execute(
                    """
                    INSERT INTO sources (id, name, slug, url, rss_url, language, is_active, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (slug) DO NOTHING
                    """,
                    s["id"], s["name"], s["slug"], s["url"], s["rss_url"],
                    s["language"], s["is_active"], s["created_at"], s["updated_at"],
                )
        for s in supa_sources:
            if s["slug"] not in local_by_slug:
                await local_pool.execute(
                    """
                    INSERT INTO sources (id, name, slug, url, rss_url, language, is_active, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (slug) DO NOTHING
                    """,
                    s["id"], s["name"], s["slug"], s["url"], s["rss_url"],
                    s["language"], s["is_active"], s["created_at"], s["updated_at"],
                )

        # Refresh after inserts
        local_sources = await local_pool.fetch("SELECT id, slug FROM sources")
        supa_sources = await supa_pool.fetch("SELECT id, slug FROM sources")
        local_id_by_slug = {s["slug"]: s["id"] for s in local_sources}
        supa_id_by_slug = {s["slug"]: s["id"] for s in supa_sources}

        # Build ID remapping: local_source_id → supa_source_id (and reverse)
        local_to_supa = {}
        supa_to_local = {}
        for slug in local_id_by_slug:
            if slug in supa_id_by_slug:
                local_to_supa[local_id_by_slug[slug]] = supa_id_by_slug[slug]
                supa_to_local[supa_id_by_slug[slug]] = local_id_by_slug[slug]

        click.echo(f"  {GREEN}✓{RESET} Sources synced ({len(local_to_supa)} matched)")

        # Helper: copy rows between pools with source_id remapping
        async def _copy_articles(src_pool, dst_pool, id_map, label):
            total = await src_pool.fetchval("SELECT COUNT(*) FROM articles")
            before = await dst_pool.fetchval("SELECT COUNT(*) FROM articles")
            offset = 0
            while offset < total:
                rows = await src_pool.fetch(article_select, batch_size, offset)
                args = []
                for r in rows:
                    mapped_id = id_map.get(r["source_id"])
                    if not mapped_id:
                        continue
                    args.append((
                        mapped_id, r["url"], r["title"], r["content"],
                        r["excerpt"], r["image_url"], r["author"], r["published_at"],
                        r["scraped_at"], r["language"], r["original_language"],
                        r["is_processed"], r["created_at"], r["updated_at"],
                    ))
                if args:
                    await dst_pool.executemany(article_sql, args)
                offset += batch_size
            after = await dst_pool.fetchval("SELECT COUNT(*) FROM articles")
            click.echo(f"  {GREEN}✓{RESET} Articles: +{after - before} to {label} ({after} total)")

        async def _copy_dead_links(src_pool, dst_pool, id_map, label):
            total = await src_pool.fetchval("SELECT COUNT(*) FROM dead_links")
            if total == 0:
                return
            before = await dst_pool.fetchval("SELECT COUNT(*) FROM dead_links")
            offset = 0
            while offset < total:
                rows = await src_pool.fetch(dl_select, batch_size, offset)
                args = []
                for r in rows:
                    mapped_id = id_map.get(r["source_id"])
                    if not mapped_id:
                        continue
                    args.append((
                        mapped_id, r["url"], r["error_type"], r["first_failed_at"],
                        r["last_checked_at"], r["retry_count"], r["created_at"],
                    ))
                if args:
                    await dst_pool.executemany(dl_sql, args)
                offset += batch_size
            after = await dst_pool.fetchval("SELECT COUNT(*) FROM dead_links")
            click.echo(f"  {GREEN}✓{RESET} Dead links: +{after - before} to {label} ({after} total)")

        # ── Phase 1: Local → Supabase ──
        click.echo(f"\n{BOLD}Phase 1: Local → Supabase{RESET}")
        await _copy_articles(local_pool, supa_pool, local_to_supa, "Supabase")
        await _copy_dead_links(local_pool, supa_pool, local_to_supa, "Supabase")

        # ── Phase 2: Supabase → Local ──
        click.echo(f"\n{BOLD}Phase 2: Supabase → Local{RESET}")
        await _copy_articles(supa_pool, local_pool, supa_to_local, "local")
        await _copy_dead_links(supa_pool, local_pool, supa_to_local, "local")

        click.echo(f"\n  {GREEN}✓{RESET} Sync complete\n")

    finally:
        await local_pool.close()
        await supa_pool.close()


@cli.command()
@click.option("--sample", default=10, help="Number of articles to review")
@click.option("--source", default=None, help="Filter by source slug")
@click.option("--since", default=None, help="Only articles published after this date (YYYY-MM-DD)")
@click.option("--prompt-version", default="v1", help="Prompt version to use (v1, v2, ...)")
@click.option("--categorize-only", is_flag=True, help="Skip QA, only categorize")
@click.option("--save", is_flag=True, help="Save passing articles to knowledge graph (Neo4j/Graphiti)")
def review(sample: int, source: str | None, since: str | None, prompt_version: str, categorize_only: bool, save: bool) -> None:
    """Review article quality using LLM agents (OpenRouter)."""
    from news_agg.agents.runner import run_review

    asyncio.run(run_review(
        sample=sample,
        source=source,
        since=since,
        prompt_version=prompt_version,
        categorize_only=categorize_only,
        save_to_graph=save,
    ))


@cli.group()
def agent() -> None:
    """Agentic pipeline commands (LangGraph orchestrator)."""
    pass


@agent.command()
@click.option("--sources", default=None, help="Comma-separated source slugs to focus on")
@click.option("--limit", default=20, help="Article limit per source for ingestion")
@click.option("--run-type", default="full_cycle", type=click.Choice(["full_cycle", "ingest_only", "review_only"]))
def run(sources: str | None, limit: int, run_type: str) -> None:
    """Run a full autonomous pipeline cycle."""
    source_list = [s.strip() for s in sources.split(",")] if sources else None
    asyncio.run(_agent_run(source_list, limit, run_type))


async def _agent_run(sources: list[str] | None, limit: int, run_type: str) -> None:
    from news_agg.agents.graph import run_agent_cycle
    from news_agg.db import close_pool

    try:
        result = await run_agent_cycle(sources=sources, limit=limit, run_type=run_type)
        status = result.get("status", "unknown")
        run_id = result.get("run_id", "?")
        click.echo(f"\n  Agent run {run_id}: {status}")
        if result.get("error"):
            click.echo(f"  Error: {result['error']}")
    finally:
        await close_pool()


@agent.command()
@click.option("--limit", default=10, help="Number of recent runs to show")
def history(limit: int) -> None:
    """Show recent agent run history."""
    asyncio.run(_agent_history(limit))


async def _agent_history(limit: int) -> None:
    from news_agg.db import close_pool, get_pool, get_recent_runs

    try:
        pool = await get_pool()
        runs = await get_recent_runs(pool, limit)

        if not runs:
            click.echo("No agent runs found.")
            return

        click.echo(f"\n{BOLD}Agent Run History{RESET}\n")
        click.echo(f"  {'Started':<20} {'Type':<15} {'Status':<12} {'Summary'}")
        click.echo(f"  {'─' * 20} {'─' * 15} {'─' * 12} {'─' * 40}")

        for run in runs:
            started = run["started_at"].strftime("%Y-%m-%d %H:%M") if run["started_at"] else "?"
            result = run.get("result", {})
            summary = result.get("summary", "")[:40] if isinstance(result, dict) else ""
            error = run.get("error_message")
            if error:
                summary = f"ERROR: {error[:35]}"
            click.echo(
                f"  {started:<20} {run['run_type']:<15} {run['status']:<12} {summary}"
            )

        click.echo()
    finally:
        await close_pool()


@agent.command()
@click.argument("run_id")
def inspect(run_id: str) -> None:
    """Inspect a specific agent run's details."""
    asyncio.run(_agent_inspect(run_id))


async def _agent_inspect(run_id: str) -> None:
    import json as json_mod

    from news_agg.db import close_pool, get_pool

    try:
        pool = await get_pool()
        row = await pool.fetchrow(
            "SELECT * FROM agent_runs WHERE id = $1::uuid", run_id
        )
        if not row:
            click.echo(f"Run {run_id} not found.")
            return

        run = dict(row)
        click.echo(f"\n{BOLD}Agent Run: {run_id}{RESET}\n")
        click.echo(f"  Type:      {run['run_type']}")
        click.echo(f"  Status:    {run['status']}")
        click.echo(f"  Thread:    {run['thread_id']}")
        click.echo(f"  Started:   {run['started_at']}")
        click.echo(f"  Completed: {run['completed_at'] or '—'}")

        if run.get("error_message"):
            click.echo(f"  Error:     {run['error_message']}")

        config = run.get("config", {})
        if config:
            click.echo(f"\n  Config: {json_mod.dumps(config, indent=2)}")

        result = run.get("result", {})
        if result:
            click.echo(f"\n  Result: {json_mod.dumps(result, indent=2)}")

        decisions = run.get("decisions", [])
        if decisions:
            click.echo(f"\n  Decisions:")
            for d in decisions:
                click.echo(f"    - {json_mod.dumps(d)}")

        click.echo()
    finally:
        await close_pool()


@cli.group()
def search() -> None:
    """Meilisearch full-text search commands."""
    pass


@search.command("sync")
@click.option("--source", default=None, help="Sync only articles from this source slug")
def search_sync(source: str | None) -> None:
    """Sync articles from PostgreSQL → Meilisearch index."""
    from news_agg.search import sync_articles

    asyncio.run(sync_articles(source_slug=source))


@search.command("query")
@click.argument("query_text")
@click.option("--limit", default=10, help="Max results")
@click.option("--source", default=None, help="Filter by source slug")
@click.option("--lang", default=None, help="Filter by language (en, si)")
@click.option("--category", default=None, help="Filter by category")
def search_query(query_text: str, limit: int, source: str | None, lang: str | None, category: str | None) -> None:
    """Search articles in Meilisearch."""
    from news_agg.search import search_articles

    result = search_articles(query_text, limit, source, lang, category)
    hits = result.get("hits", [])
    est_total = result.get("estimatedTotalHits", 0)
    time_ms = result.get("processingTimeMs", 0)

    click.echo(f"\n{BOLD}Search: \"{query_text}\"{RESET}  ({est_total} results, {time_ms}ms)\n")
    for i, hit in enumerate(hits, 1):
        title = hit.get("title", "(no title)")[:70]
        source_name = hit.get("source_name", "?")
        published = hit.get("published_at", "")[:10] if hit.get("published_at") else "—"
        click.echo(f"  {i}. {title}")
        click.echo(f"     {DIM}{source_name} | {published} | {hit.get('language', '?')}{RESET}")
    click.echo()


@search.command("stats")
def search_stats() -> None:
    """Show Meilisearch index stats."""
    from news_agg.search import get_index_stats

    stats = get_index_stats()
    click.echo(f"\n{BOLD}Meilisearch Index{RESET}")
    click.echo(f"  Documents: {stats['number_of_documents']}")
    click.echo(f"  Indexing:  {stats['is_indexing']}\n")


@cli.group()
def snapshot() -> None:
    """R2 database snapshot commands (push/pull between PCs)."""
    pass


@snapshot.command("push")
@click.option("--label", default=None, help="Label for this snapshot (e.g. 'pc-b', 'pre-migration')")
@click.option("--all", "push_all_flag", is_flag=True, help="Push PostgreSQL + Neo4j (full data sync)")
@click.option("--neo4j-only", is_flag=True, help="Push only Neo4j snapshot")
def snapshot_push(label: str | None, push_all_flag: bool, neo4j_only: bool) -> None:
    """Dump data stores → compress → upload to Cloudflare R2."""
    from news_agg.snapshot import push_all, push_neo4j, push_pg

    if push_all_flag:
        results = push_all(label)
        click.echo(f"\n  Snapshots uploaded:")
        for store, key in results.items():
            click.echo(f"    {store}: {key or 'skipped'}")
        click.echo()
    elif neo4j_only:
        key = push_neo4j(label)
        click.echo(f"\n  Neo4j snapshot uploaded: {key}\n")
    else:
        key = push_pg(label)
        click.echo(f"\n  PostgreSQL snapshot uploaded: {key}\n")


@snapshot.command("pull")
@click.option("--key", default=None, help="Specific snapshot key (default: latest)")
@click.option("--all", "pull_all_flag", is_flag=True, help="Pull PostgreSQL + Neo4j + rebuild Meilisearch")
@click.option("--neo4j-only", is_flag=True, help="Pull only Neo4j snapshot")
@click.option("--no-search", is_flag=True, help="Skip Meilisearch rebuild (with --all)")
def snapshot_pull(key: str | None, pull_all_flag: bool, neo4j_only: bool, no_search: bool) -> None:
    """Download snapshots from R2 → restore locally."""
    if pull_all_flag:
        from news_agg.snapshot import pull_all
        results = asyncio.run(pull_all(rebuild_search=not no_search))
        click.echo(f"\n  Restore complete:")
        for store, status in results.items():
            click.echo(f"    {store}: {status}")
        click.echo()
    elif neo4j_only:
        from news_agg.snapshot import pull_neo4j
        pull_neo4j(key)
    else:
        from news_agg.snapshot import pull_pg
        pull_pg(key)


@snapshot.command("list")
@click.option("--limit", default=20, help="Number of snapshots to list")
def snapshot_list(limit: int) -> None:
    """List available snapshots in R2 (PostgreSQL + Neo4j)."""
    from news_agg.snapshot import list_snapshots

    snapshots = list_snapshots(limit)
    if not snapshots:
        click.echo("No snapshots found.")
        return

    click.echo(f"\n{BOLD}R2 Snapshots{RESET}\n")
    click.echo(f"  {'Type':<12} {'Key':<45} {'Size':>8} {'Modified'}")
    click.echo(f"  {'─' * 12} {'─' * 45} {'─' * 8} {'─' * 20}")
    for s in snapshots:
        click.echo(f"  {s['type']:<12} {s['key']:<45} {s['size_mb']:>6.1f}MB {s['last_modified']}")
    click.echo()


@cli.command("db-migrate")
def db_migrate() -> None:
    """Apply database migrations (for existing databases)."""
    asyncio.run(_db_migrate())


async def _db_migrate() -> None:
    from pathlib import Path

    import asyncpg

    from news_agg.config import settings

    migrations_dir = Path(__file__).resolve().parents[3] / "docker" / "migrations"
    if not migrations_dir.exists():
        click.echo("No migrations directory found.")
        return

    migrations = sorted(migrations_dir.glob("*.sql"))
    if not migrations:
        click.echo("No migration files found.")
        return

    conn = await asyncpg.connect(settings.database_url)
    try:
        for migration in migrations:
            click.echo(f"  Applying {migration.name}...")
            sql = migration.read_text()
            await conn.execute(sql)
            click.echo(f"  {GREEN}✓{RESET} {migration.name}")
        click.echo(f"\n  {GREEN}✓{RESET} All migrations applied")
    except Exception as e:
        click.echo(f"  {RED}✗{RESET} Migration failed: {e}")
    finally:
        await conn.close()


if __name__ == "__main__":
    cli()
