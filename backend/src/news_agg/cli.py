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

        # 2. Copy sources (ON CONFLICT DO NOTHING — preserve local seeds)
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
        click.echo(f"  {GREEN}✓{RESET} {len(src_sources)} sources synced")

        # 3. Copy articles in batches
        total = await src_pool.fetchval("SELECT COUNT(*) FROM articles")
        dst_before = await dst_pool.fetchval("SELECT COUNT(*) FROM articles")
        click.echo(f"  {DIM}Copying {total} articles ({dst_before} already in local)...{RESET}")

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
        click.echo(f"  {GREEN}✓{RESET} {inserted} new articles copied ({dst_after} total in local)")

        # 4. Copy dead_links in batches
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
                args = [
                    (r["source_id"], r["url"], r["error_type"], r["first_failed_at"],
                     r["last_checked_at"], r["retry_count"], r["created_at"])
                    for r in rows
                ]
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


if __name__ == "__main__":
    cli()
