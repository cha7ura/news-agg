"""Ingestion pipeline orchestrator.

Wires RSS/listing discovery, Playwright scraping, text processing, and DB writes
into a single ingestion flow. Ported from ground-news/scripts/pipeline.ts
lines 937-1208 (runIngest).

Supports parallel scraping via asyncio.Semaphore — multiple browser pages
scrape different articles concurrently while respecting rate limits.
"""

from __future__ import annotations

import asyncio
import re
from uuid import UUID

from news_agg.config import settings
from news_agg.db import (
    get_active_sources,
    get_article_stats,
    get_dead_urls,
    get_existing_urls,
    get_pool,
    get_recent_titles,
    get_source_by_slug,
    insert_article,
    record_dead_link,
    remove_dead_link,
)
from news_agg.models import ArticleCreate, RSSItem, ScrapeError, Source
from news_agg.scheduler import IntelligentScheduler
from news_agg.scraper.article import scrape_article_page
from news_agg.scraper.browser import close_playwright, connect_browser, create_context
from news_agg.scraper.listing import scrape_listing_page
from news_agg.scraper.rss import fetch_rss
from news_agg.source_config import get_scheduling_config
from news_agg.text.dedup import normalize_title
from news_agg.text.normalize import normalize_text
from news_agg.utils.logging import GREEN, RED, YELLOW, BOLD, DIM, RESET, get_logger
from news_agg.utils.rate_limit import RateLimiter

log = get_logger()

# URL patterns to skip (pipeline.ts lines 1085-1093)
_SKIP_URL_PATTERNS = re.compile(
    r"(\.(jpg|jpeg|png|gif|svg|webp|pdf)$"
    r"|/feed/?$"
    r"|/print/?$"
    r"|/wp-content/uploads/"
    r"|/(category|tag|author|page)/"
    r"|/(hot-news|news_archive|sports|entertainment-news)/?$"
    r"|/\?mode=(beauti|head))",
    re.IGNORECASE,
)


def _should_skip_url(url: str) -> bool:
    return bool(_SKIP_URL_PATTERNS.search(url))


async def run_ingest(
    source_slug: str | None = None,
    limit: int = 20,
    concurrency: int = 1,
) -> dict:
    """Run the ingestion pipeline for one or all sources.

    Args:
        source_slug: Specific source to ingest (None = all active sources).
        limit: Max articles per source.
        concurrency: Number of concurrent browser pages for scraping.

    Returns summary dict with counts of inserted, skipped articles.
    """
    log.info(f"{BOLD}INGEST{RESET} — fetching articles (concurrency={concurrency})")

    pool = await get_pool()

    # Get sources to process
    if source_slug:
        source = await get_source_by_slug(pool, source_slug)
        if not source:
            log.error(f"Source not found: {source_slug}")
            return {"error": f"Source not found: {source_slug}"}
        sources = [source]
    else:
        sources = await get_active_sources(pool)

    if not sources:
        log.warning("No active sources found")
        return {"inserted": 0, "skipped_no_date": 0, "skipped_duplicate": 0}

    log.info(f"Found {len(sources)} active source(s)")

    # Connect browser once for the entire ingest run
    browser = None
    try:
        browser = await connect_browser()
        log.info(f"{GREEN}✓{RESET} Playwright connected")
    except Exception as e:
        log.error(f"{RED}✗{RESET} Playwright connection failed: {e}")
        log.warning(f"{YELLOW}–{RESET} Continuing without article page scraping")

    try:
        # Single source → use original sequential flow (backward compat)
        if len(sources) == 1:
            result = await _ingest_source(pool, browser, sources[0], limit, concurrency)
            return result

        # Multi-source → intelligent interleaved scheduling
        result = await _ingest_interleaved(pool, browser, sources, limit, concurrency)
        return result
    finally:
        if browser:
            await browser.close()
        await close_playwright()


async def _ingest_interleaved(
    pool,
    browser,
    sources: list[Source],
    limit: int,
    concurrency: int,
) -> dict:
    """Multi-source ingest with intelligent scheduling.

    Discovers URLs from all sources concurrently, then scrapes interleaved
    across sources — workers pull from whichever source's rate limit has
    cooled down, eliminating idle time.
    """
    scheduler = IntelligentScheduler(browser, pool, global_concurrency=concurrency)

    # Register all sources with their scheduling configs
    for source in sources:
        sched = get_scheduling_config(source.slug)
        scheduler.register_source(
            source=source,
            rate_limit_ms=sched["rate_limit_ms"] or settings.rate_limit_ms,
            max_concurrency=sched["max_concurrency"] or concurrency,
            priority=sched["priority"],
        )

    # Per-source dedup state
    existing_urls: dict[str, set[str]] = {}
    existing_titles: dict[str, set[str]] = {}
    counts: dict[str, dict[str, int]] = {}

    async def _discover_and_enqueue(source: Source) -> None:
        """Producer: discover URLs for one source, dedup, enqueue."""
        slug = source.slug
        try:
            items = await _discover_articles(browser, source, limit)
            if not items:
                return

            urls = [item.link for item in items[:limit]]
            existing = await get_existing_urls(pool, source.id, urls)
            dead = await get_dead_urls(pool, source.id, urls)
            recent_raw = await get_recent_titles(pool, source.id)
            titles = {normalize_title(t) for t in recent_raw if len(normalize_title(t)) > 10}

            existing_urls[slug] = existing
            existing_titles[slug] = titles
            counts[slug] = {"inserted": 0, "skipped_no_date": 0, "skipped_duplicate": 0}

            filtered = []
            for item in items[:limit]:
                if item.link in existing or item.link in dead:
                    continue
                norm = normalize_title(item.title)
                if norm and len(norm) > 10 and norm in titles:
                    continue
                if _should_skip_url(item.link):
                    continue
                filtered.append(item)

            if filtered:
                log.info(f"  {DIM}[{slug}] {len(filtered)} new articles queued{RESET}")
                await scheduler.enqueue(slug, filtered)
            else:
                log.info(f"  {DIM}[{slug}] all articles already in DB{RESET}")
        except Exception as e:
            log.error(f"  {RED}✗{RESET} [{slug}] discovery failed: {e}")
        finally:
            scheduler.mark_discovery_done(slug)

    # Run discovery for all sources concurrently + start workers immediately
    discovery_tasks = [asyncio.create_task(_discover_and_enqueue(s)) for s in sources]
    worker_task = asyncio.create_task(
        scheduler.run(existing_urls, existing_titles, counts)
    )

    await asyncio.gather(*discovery_tasks)
    await worker_task
    await scheduler.cleanup()

    # Aggregate results
    total = {"inserted": 0, "skipped_no_date": 0, "skipped_duplicate": 0}
    for slug, c in counts.items():
        total["inserted"] += c["inserted"]
        total["skipped_no_date"] += c["skipped_no_date"]
        total["skipped_duplicate"] += c["skipped_duplicate"]
        if c["inserted"] > 0:
            log.info(f"  {GREEN}▸{RESET} {slug}: {c['inserted']} new articles")

    log.info(
        f"{GREEN}▸{RESET} Ingest complete: {total['inserted']} inserted, "
        f"{total['skipped_no_date']} skipped (no date), "
        f"{total['skipped_duplicate']} skipped (duplicate)"
    )
    return total


async def _ingest_source(
    pool,
    browser,
    source: Source,
    limit: int,
    concurrency: int,
) -> dict:
    """Ingest articles from a single source."""
    log.info(f"{DIM}Source: {source.name} ({source.slug}){RESET}")

    # Step 1: Discover article URLs (RSS or listing page)
    rss_items = await _discover_articles(browser, source, limit)
    if not rss_items:
        log.warning(f"  {YELLOW}–{RESET} No articles found for {source.slug}")
        return {"inserted": 0, "skipped_no_date": 0, "skipped_duplicate": 0}

    log.info(f"  Found {len(rss_items)} article links")

    # Step 2: Deduplicate against DB (pipeline.ts lines 1032-1054)
    urls = [item.link for item in rss_items[:limit]]
    existing_urls = await get_existing_urls(pool, source.id, urls)
    dead_urls = await get_dead_urls(pool, source.id, urls)

    # Get recent titles for title-based dedup
    recent_titles_raw = await get_recent_titles(pool, source.id)
    existing_titles = {
        normalize_title(t) for t in recent_titles_raw if len(normalize_title(t)) > 10
    }

    # Filter to only new articles before scraping
    items_to_scrape: list[RSSItem] = []
    for item in rss_items[:limit]:
        if item.link in existing_urls:
            continue
        if item.link in dead_urls:
            continue
        norm_title = normalize_title(item.title)
        if norm_title and len(norm_title) > 10 and norm_title in existing_titles:
            continue
        if _should_skip_url(item.link):
            continue
        items_to_scrape.append(item)

    if not items_to_scrape:
        log.info(f"  {DIM}All articles already in DB — nothing to scrape{RESET}")
        return {"inserted": 0, "skipped_no_date": 0, "skipped_duplicate": 0}

    log.info(f"  {len(items_to_scrape)} new articles to scrape")

    # Step 3: Scrape articles (parallel or sequential)
    if not browser:
        log.warning(f"  {YELLOW}–{RESET} No browser — skipping source (needs Playwright)")
        return {"inserted": 0, "skipped_no_date": 0, "skipped_duplicate": 0}

    # Sources without RSS (e.g. Cloudflare-protected) use a fresh browser context
    # per article to avoid session-level rate limiting. RSS sources share a context.
    use_fresh_ctx = not source.rss_url
    context = None if use_fresh_ctx else await create_context(browser)
    # The scraper receives either the browser (fresh ctx) or the shared context
    scraper_target = browser if use_fresh_ctx else context

    rate_limiter = RateLimiter(settings.rate_limit_ms)
    semaphore = asyncio.Semaphore(concurrency)

    # Shared mutable counters — use a dict so concurrent tasks can update it
    counts = {"inserted": 0, "skipped_no_date": 0, "skipped_duplicate": 0}
    # Lock for DB writes and counter updates
    db_lock = asyncio.Lock()

    async def _scrape_one(item: RSSItem) -> None:
        """Scrape a single article, rate-limited and semaphore-guarded."""
        async with semaphore:
            await rate_limiter.wait()

            scraped = await scrape_article_page(scraper_target, item.link, item.pub_date, source.slug)
            if isinstance(scraped, ScrapeError):
                log.warning(
                    f"  {RED}✗{RESET} {item.title[:50]}... ({scraped.error_type})"
                )
                await record_dead_link(pool, source.id, scraped.url, scraped.error_type)
                return
            if not scraped or not scraped.content or len(scraped.content) < 100:
                log.warning(
                    f"  {RED}✗{RESET} {item.title[:50]}... (scrape failed or too short)"
                )
                return
            # Successful scrape — remove from dead_links if it was a retry
            await remove_dead_link(pool, item.link)

            article_title = scraped.title or normalize_text(item.title)

            if not scraped.published_at:
                counts["skipped_no_date"] += 1
                log.warning(f"  {YELLOW}–{RESET} {article_title[:50]}... (NO DATE — skipped)")
                return

            article = ArticleCreate(
                source_id=source.id,
                url=item.link,
                title=article_title,
                content=scraped.content,
                excerpt=scraped.excerpt,
                image_url=scraped.image_url or item.image_url,
                author=scraped.author,
                published_at=scraped.published_at,
                language=source.language,
                original_language=source.language,
            )

            async with db_lock:
                norm_title = normalize_title(item.title)
                if norm_title and len(norm_title) > 10 and norm_title in existing_titles:
                    counts["skipped_duplicate"] += 1
                    return

                article_id = await insert_article(pool, article)
                if article_id:
                    log.info(
                        f"  {GREEN}✓{RESET} {article_title[:50]}... "
                        f"({len(scraped.content)} chars)"
                    )
                    counts["inserted"] += 1
                    existing_titles.add(norm_title)
                    existing_urls.add(item.link)
                else:
                    counts["skipped_duplicate"] += 1

    try:
        tasks = [_scrape_one(item) for item in items_to_scrape]
        await asyncio.gather(*tasks)
    finally:
        if context:
            await context.close()

    if counts["inserted"] > 0:
        log.info(f"  {GREEN}▸{RESET} {source.name}: {counts['inserted']} new articles")
    if counts["skipped_no_date"] > 0:
        log.info(f"  {YELLOW}▸{RESET} {source.name}: {counts['skipped_no_date']} skipped (no date)")
    if counts["skipped_duplicate"] > 0:
        log.info(f"  {DIM}▸ {source.name}: {counts['skipped_duplicate']} skipped (duplicate){RESET}")

    return counts


async def _discover_articles(
    browser,
    source: Source,
    limit: int,
) -> list[RSSItem]:
    """Discover article URLs via RSS or listing page fallback.

    Pipeline.ts lines 1008-1027: Try RSS first, fall back to listing page.
    """
    items: list[RSSItem] = []

    # Try RSS first if available
    if source.rss_url:
        try:
            items = await fetch_rss(source.rss_url)
            if items:
                log.info(f"  {GREEN}✓{RESET} RSS: {len(items)} articles")
        except Exception as e:
            log.error(f"  {RED}✗{RESET} RSS fetch failed: {e}")

    # Fallback to listing page scrape
    if not items and browser:
        log.info(f"  {YELLOW}–{RESET} RSS empty/failed, trying listing page scrape...")
        items = await scrape_listing_page(browser, source.url, source.slug, limit)
        if items:
            log.info(f"  {GREEN}✓{RESET} Listing page fallback: {len(items)} article links")
        else:
            log.warning(f"  {YELLOW}–{RESET} No articles found from listing page either")

    return items
