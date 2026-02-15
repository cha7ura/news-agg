"""Historical backfill — crawl paginated archive pages, sweep NID ranges, or date-based archives.

Three modes:
  Archive: Crawl paginated listing pages (e.g. ?pageno=N)
  NID sweep: Iterate through sequential article IDs for exhaustive coverage
  Date sweep: Iterate through calendar dates for date-based archive pages (e.g. /YYYY/MM/DD)

Usage:
    news-agg ingest --source ada-derana-en --backfill --pages 10 --concurrency 5
    news-agg ingest --source ada-derana-en --nid-sweep --concurrency 5
    news-agg ingest --source newsfirst-en --date-sweep --concurrency 3
"""

from __future__ import annotations

import asyncio
import re
from datetime import date, timedelta

from news_agg.config import settings
from news_agg.db import (
    get_active_sources,
    get_all_dead_urls,
    get_all_source_urls,
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
from news_agg.pipeline import _should_skip_url
from news_agg.scraper.article import scrape_article_page
from news_agg.scraper.browser import close_playwright, connect_browser, create_context
from news_agg.scraper.listing import _EXTRACT_LINKS_JS
from news_agg.scheduler import IntelligentScheduler
from news_agg.source_config import get_archive_patterns, get_article_url_patterns, get_backfill_methods, get_date_sweep_config, get_nid_sweep_config, get_scheduling_config
from news_agg.text.dedup import normalize_title
from news_agg.text.normalize import normalize_text
from news_agg.utils.logging import GREEN, RED, YELLOW, BOLD, DIM, RESET, get_logger
from news_agg.utils.rate_limit import RateLimiter

log = get_logger()


async def _crawl_archive_pages(
    browser,
    source: Source,
    pages: int,
) -> list[RSSItem]:
    """Crawl multiple paginated archive pages to discover article URLs.

    Uses archive patterns from sources.yaml — crawls all sections (hot-news, sports, etc.).
    """
    archive_patterns = get_archive_patterns(source.slug)
    if not archive_patterns:
        log.warning(f"  {YELLOW}–{RESET} No archive patterns configured for {source.slug}")
        return []

    all_items: list[RSSItem] = []
    seen_urls: set[str] = set()
    article_patterns = get_article_url_patterns(source.slug)
    # Cloudflare-protected sites need fresh context per page navigation
    needs_fresh_ctx = not source.rss_url

    from urllib.parse import urlparse

    if needs_fresh_ctx:
        # Fresh context per page — prevents Cloudflare session-level blocking
        for ap in archive_patterns:
            section = ap["section"]
            pattern = ap["pattern"]
            max_pages = min(pages, ap["max_pages"])
            page_start = ap.get("page_start", 1)
            page_step = ap.get("page_step", 1)
            log.info(f"  {BOLD}Section: {section}{RESET}")

            consecutive_empty = 0
            for i in range(max_pages):
                page_val = page_start + i * page_step
                url = pattern.format(page=page_val)
                log.info(f"  {DIM}Crawling {section} page {i + 1}/{max_pages}...{RESET}")

                ctx = await create_context(browser)
                try:
                    pg = await ctx.new_page()
                    await pg.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await pg.wait_for_timeout(2000)

                    title = await pg.title()
                    if "just a moment" in title.lower():
                        log.info(f"  Cloudflare challenge, waiting...")
                        for _ in range(10):
                            await pg.wait_for_timeout(1000)
                            title = await pg.title()
                            if "just a moment" not in title.lower():
                                break
                        else:
                            log.warning(f"  Cloudflare did not resolve — skipping page")
                            await pg.close()
                            continue

                    parsed = urlparse(url)
                    base_url = f"{parsed.scheme}://{parsed.netloc}"
                    links = await pg.evaluate(
                        _EXTRACT_LINKS_JS,
                        {"baseUrl": base_url, "slug": source.slug, "articleUrlPatterns": article_patterns},
                    )
                    await pg.close()

                    new_count = 0
                    for link in links:
                        if link["url"] not in seen_urls:
                            seen_urls.add(link["url"])
                            all_items.append(RSSItem(title=link["title"], link=link["url"]))
                            new_count += 1

                    log.info(f"  {section} p{i + 1}: {new_count} new links (total: {len(all_items)})")

                    if not links:
                        log.info(f"  {YELLOW}–{RESET} No links on {section} page {i + 1} — stopping")
                        break

                    # Stop section early if 3 consecutive pages yield 0 new links
                    if new_count == 0:
                        consecutive_empty += 1
                        if consecutive_empty >= 3:
                            log.info(f"  {DIM}3 pages with 0 new links — skipping rest of {section}{RESET}")
                            break
                    else:
                        consecutive_empty = 0
                except Exception as e:
                    log.error(f"  {RED}✗{RESET} Archive {section} page {i + 1} failed: {e}")
                finally:
                    await ctx.close()
    else:
        # Shared context — fast path for non-Cloudflare sources
        context = await create_context(browser)
        try:
            page = await context.new_page()
            for ap in archive_patterns:
                section = ap["section"]
                pattern = ap["pattern"]
                max_pages = min(pages, ap["max_pages"])
                page_start = ap.get("page_start", 1)
                page_step = ap.get("page_step", 1)
                log.info(f"  {BOLD}Section: {section}{RESET}")

                consecutive_empty = 0
                for i in range(max_pages):
                    page_val = page_start + i * page_step
                    url = pattern.format(page=page_val)
                    log.info(f"  {DIM}Crawling {section} page {i + 1}/{max_pages}...{RESET}")

                    try:
                        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                        await page.wait_for_timeout(2000)

                        parsed = urlparse(url)
                        base_url = f"{parsed.scheme}://{parsed.netloc}"
                        links = await page.evaluate(
                            _EXTRACT_LINKS_JS,
                            {"baseUrl": base_url, "slug": source.slug, "articleUrlPatterns": article_patterns},
                        )

                        new_count = 0
                        for link in links:
                            if link["url"] not in seen_urls:
                                seen_urls.add(link["url"])
                                all_items.append(RSSItem(title=link["title"], link=link["url"]))
                                new_count += 1

                        log.info(f"  {section} p{i + 1}: {new_count} new links (total: {len(all_items)})")

                        if not links:
                            log.info(f"  {YELLOW}–{RESET} No links on {section} page {i + 1} — stopping")
                            break

                        if new_count == 0:
                            consecutive_empty += 1
                            if consecutive_empty >= 3:
                                log.info(f"  {DIM}3 pages with 0 new links — skipping rest of {section}{RESET}")
                                break
                        else:
                            consecutive_empty = 0
                    except Exception as e:
                        log.error(f"  {RED}✗{RESET} Archive {section} page {i + 1} failed: {e}")
                        continue

            await page.close()
        finally:
            await context.close()

    return all_items


async def run_backfill(
    source_slug: str | None = None,
    pages: int = 5,
    concurrency: int = 3,
) -> dict:
    """Run historical backfill for one or all sources.

    Crawls paginated archive pages, deduplicates, then scrapes articles in parallel.
    """
    log.info(
        f"{BOLD}BACKFILL{RESET} — crawling {pages} archive pages "
        f"(concurrency={concurrency})"
    )

    pool = await get_pool()

    if source_slug:
        source = await get_source_by_slug(pool, source_slug)
        if not source:
            log.error(f"Source not found: {source_slug}")
            return {"error": f"Source not found: {source_slug}"}
        sources = [source]
    else:
        sources = await get_active_sources(pool)

    browser = None
    try:
        browser = await connect_browser()
        log.info(f"{GREEN}✓{RESET} Playwright connected")
    except Exception as e:
        log.error(f"{RED}✗{RESET} Playwright connection failed: {e}")
        return {"error": f"Playwright connection failed: {e}"}

    total_inserted = 0
    total_skipped = 0

    try:
        for source in sources:
            if not get_archive_patterns(source.slug):
                log.warning(f"  {YELLOW}–{RESET} No archive patterns configured for {source.slug} — skipping")
                continue

            # Step 1: Crawl archive pages to discover URLs
            discovered = await _crawl_archive_pages(browser, source, pages)
            if not discovered:
                continue

            log.info(f"  {GREEN}✓{RESET} Discovered {len(discovered)} article URLs total")

            # Step 2: Deduplicate against DB
            urls = [item.link for item in discovered]
            existing_urls = await get_existing_urls(pool, source.id, urls)
            dead_urls = await get_dead_urls(pool, source.id, urls)

            recent_titles_raw = await get_recent_titles(pool, source.id, days=365)
            existing_titles = {
                normalize_title(t) for t in recent_titles_raw if len(normalize_title(t)) > 10
            }

            items_to_scrape = []
            for item in discovered:
                if item.link in existing_urls:
                    continue
                if item.link in dead_urls:
                    continue
                if _should_skip_url(item.link):
                    continue
                norm_title = normalize_title(item.title)
                if norm_title and len(norm_title) > 10 and norm_title in existing_titles:
                    continue
                items_to_scrape.append(item)

            skipped = len(discovered) - len(items_to_scrape)
            total_skipped += skipped
            log.info(
                f"  {len(items_to_scrape)} new articles to scrape "
                f"({skipped} already in DB)"
            )

            if not items_to_scrape:
                continue

            # Step 3: Scrape articles in parallel
            # Sources without RSS (Cloudflare-protected) use fresh context per page
            use_fresh_ctx = not source.rss_url
            context = None if use_fresh_ctx else await create_context(browser)
            scraper_target = browser if use_fresh_ctx else context
            rate_limiter = RateLimiter(settings.rate_limit_ms)
            semaphore = asyncio.Semaphore(concurrency)
            db_lock = asyncio.Lock()
            inserted = 0

            failed = 0
            no_date = 0

            async def _scrape_one(item: RSSItem) -> None:
                nonlocal inserted, failed, no_date
                async with semaphore:
                    await rate_limiter.wait()

                    scraped = await scrape_article_page(scraper_target, item.link, item.pub_date, source.slug)
                    if isinstance(scraped, ScrapeError):
                        failed += 1
                        log.debug(f"  {RED}✗{RESET} {item.title[:40]}... ({scraped.error_type})")
                        await record_dead_link(pool, source.id, scraped.url, scraped.error_type)
                        return
                    if not scraped or not scraped.content or len(scraped.content) < 100:
                        failed += 1
                        log.debug(f"  {RED}✗{RESET} {item.title[:40]}... (scrape failed)")
                        return
                    # Successful scrape — remove from dead_links if it was a retry
                    await remove_dead_link(pool, item.link)

                    article_title = scraped.title or normalize_text(item.title)

                    if not scraped.published_at:
                        no_date += 1
                        log.debug(f"  {YELLOW}–{RESET} {article_title[:40]}... (no date)")
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
                        article_id = await insert_article(pool, article)
                        if article_id:
                            inserted += 1
                            existing_urls.add(item.link)
                            if inserted % 10 == 0:
                                log.info(
                                    f"  {GREEN}▸{RESET} Progress: {inserted} articles inserted..."
                                )

            try:
                tasks = [_scrape_one(item) for item in items_to_scrape]
                await asyncio.gather(*tasks)
            finally:
                if context:
                    await context.close()

            total_inserted += inserted
            log.info(
                f"  {GREEN}▸{RESET} {source.name}: {inserted} inserted, "
                f"{failed} failed, {no_date} no date"
            )

    finally:
        if browser:
            await browser.close()
        await close_playwright()

    log.info(
        f"{GREEN}▸{RESET} Backfill complete: {total_inserted} inserted, "
        f"{total_skipped} skipped (already in DB)"
    )
    return {"inserted": total_inserted, "skipped": total_skipped}


async def run_nid_sweep(
    source_slug: str | None = None,
    concurrency: int = 3,
    browser=None,
) -> dict:
    """Sweep through sequential NID ranges to discover every article.

    Iterates from start to end nid, navigates to each URL, captures the
    canonical URL after redirects, and scrapes if not already in DB.
    Stops a sweep pattern after max_consecutive_404 misses.
    """
    pool = await get_pool()

    if source_slug:
        source = await get_source_by_slug(pool, source_slug)
        if not source:
            log.error(f"Source not found: {source_slug}")
            return {"error": f"Source not found: {source_slug}"}
        sources = [source]
    else:
        sources = await get_active_sources(pool)

    own_browser = browser is None
    if own_browser:
        try:
            browser = await connect_browser()
            log.info(f"{GREEN}✓{RESET} Playwright connected")
        except Exception as e:
            log.error(f"{RED}✗{RESET} Playwright connection failed: {e}")
            return {"error": f"Playwright connection failed: {e}"}

    total_inserted = 0
    total_skipped = 0
    total_not_found = 0

    try:
        for source in sources:
            sweep_configs = get_nid_sweep_config(source.slug)
            if not sweep_configs:
                log.warning(f"  {YELLOW}–{RESET} No nid_sweep configured for {source.slug}")
                continue

            # Pre-load all existing URLs and dead URLs for this source
            existing_urls = await get_all_source_urls(pool, source.id)
            dead_urls = await get_all_dead_urls(pool, source.id)
            log.info(
                f"  {DIM}{source.name}: {len(existing_urls)} articles in DB, "
                f"{len(dead_urls)} dead links{RESET}"
            )

            for sweep in sweep_configs:
                url_pattern = sweep["url_pattern"]
                start = sweep["start"]
                end = sweep["end"]
                max_404 = sweep.get("max_consecutive_404", 50)

                log.info(
                    f"{BOLD}NID SWEEP{RESET} — {source.name} "
                    f"nid {start}→{end} ({end - start + 1} to check)"
                )

                context = await create_context(browser)
                rate_limiter = RateLimiter(settings.rate_limit_ms)
                semaphore = asyncio.Semaphore(concurrency)
                db_lock = asyncio.Lock()

                inserted = 0
                skipped = 0
                not_found = 0
                consecutive_404 = 0

                # Process in batches to show progress and manage memory
                batch_size = 50
                for batch_start in range(start, end + 1, batch_size):
                    batch_end = min(batch_start + batch_size, end + 1)
                    nids = list(range(batch_start, batch_end))

                    # Quick pre-filter: skip nids whose URL is already in DB or dead
                    nids_to_check = []
                    for nid in nids:
                        candidate_url = url_pattern.format(nid=nid)
                        if candidate_url in existing_urls:
                            skipped += 1
                        elif candidate_url in dead_urls:
                            skipped += 1
                        else:
                            nids_to_check.append(nid)

                    if not nids_to_check:
                        continue

                    async def _sweep_one(nid: int) -> None:
                        nonlocal inserted, skipped, not_found, consecutive_404
                        async with semaphore:
                            if consecutive_404 >= max_404:
                                return

                            await rate_limiter.wait()
                            url = url_pattern.format(nid=nid)

                            scraped = await scrape_article_page(
                                context, url, source_slug=source.slug
                            )

                            if isinstance(scraped, ScrapeError):
                                not_found += 1
                                consecutive_404 += 1
                                await record_dead_link(pool, source.id, scraped.url, scraped.error_type)
                                dead_urls.add(scraped.url)
                                return
                            if not scraped or not scraped.content or len(scraped.content) < 100:
                                not_found += 1
                                consecutive_404 += 1
                                return

                            # Reset consecutive 404 counter — we found a valid article
                            consecutive_404 = 0
                            # Remove from dead_links if this was a retry
                            await remove_dead_link(pool, url)

                            # Use canonical URL after redirect for dedup and storage
                            canonical_url = scraped.final_url or url

                            async with db_lock:
                                if canonical_url in existing_urls:
                                    skipped += 1
                                    return

                                article_title = scraped.title or f"Article {nid}"

                                if not scraped.published_at:
                                    not_found += 1
                                    log.debug(
                                        f"  {YELLOW}–{RESET} nid={nid} (no date)"
                                    )
                                    return

                                article = ArticleCreate(
                                    source_id=source.id,
                                    url=canonical_url,
                                    title=article_title,
                                    content=scraped.content,
                                    excerpt=scraped.excerpt,
                                    image_url=scraped.image_url,
                                    author=scraped.author,
                                    published_at=scraped.published_at,
                                    language=source.language,
                                    original_language=source.language,
                                )

                                article_id = await insert_article(pool, article)
                                if article_id:
                                    inserted += 1
                                    existing_urls.add(canonical_url)
                                    if inserted % 10 == 0:
                                        log.info(
                                            f"  {GREEN}▸{RESET} Progress: {inserted} inserted "
                                            f"(nid ~{nid}, {not_found} 404s)"
                                        )
                                else:
                                    skipped += 1

                    tasks = [_sweep_one(nid) for nid in nids_to_check]
                    await asyncio.gather(*tasks)

                    if consecutive_404 >= max_404:
                        log.info(
                            f"  {YELLOW}–{RESET} {max_404} consecutive 404s at nid={batch_start} "
                            f"— stopping sweep"
                        )
                        break

                    # Batch progress
                    if (batch_start - start) % 500 == 0 and batch_start > start:
                        log.info(
                            f"  {DIM}Sweep progress: nid {batch_start}/{end} "
                            f"({inserted} inserted, {skipped} skipped, {not_found} 404s){RESET}"
                        )

                await context.close()

                total_inserted += inserted
                total_skipped += skipped
                total_not_found += not_found

                log.info(
                    f"  {GREEN}▸{RESET} {source.name}: {inserted} inserted, "
                    f"{skipped} skipped, {not_found} not found/no date"
                )

    finally:
        if own_browser and browser:
            await browser.close()
            await close_playwright()

    log.info(
        f"{GREEN}▸{RESET} NID sweep complete: {total_inserted} inserted, "
        f"{total_skipped} skipped, {total_not_found} not found"
    )
    return {
        "inserted": total_inserted,
        "skipped": total_skipped,
        "not_found": total_not_found,
    }


async def run_date_sweep(
    source_slug: str | None = None,
    concurrency: int = 3,
    days: int | None = None,
    browser=None,
) -> dict:
    """Sweep through calendar dates to discover articles from date-based archive pages.

    For sources like NewsFirst where /YYYY/MM/DD lists all articles published that day.
    Iterates from start_date (config) to today, loading each daily archive page
    to extract article links, then scrapes new articles in parallel.

    Args:
        source_slug: Specific source to sweep (None = all with date_sweep config).
        concurrency: Number of concurrent browser pages for scraping.
        days: Limit to last N days (None = full range from start_date).
        browser: Optional shared browser — if None, creates/closes its own.
    """
    pool = await get_pool()

    if source_slug:
        source = await get_source_by_slug(pool, source_slug)
        if not source:
            log.error(f"Source not found: {source_slug}")
            return {"error": f"Source not found: {source_slug}"}
        sources = [source]
    else:
        sources = await get_active_sources(pool)

    own_browser = browser is None
    if own_browser:
        try:
            browser = await connect_browser()
            log.info(f"{GREEN}✓{RESET} Playwright connected")
        except Exception as e:
            log.error(f"{RED}✗{RESET} Playwright connection failed: {e}")
            return {"error": f"Playwright connection failed: {e}"}

    total_inserted = 0
    total_skipped = 0

    try:
        for source in sources:
            sweep_config = get_date_sweep_config(source.slug)
            if not sweep_config:
                log.warning(f"  {YELLOW}–{RESET} No date_sweep configured for {source.slug}")
                continue

            url_pattern = sweep_config["url_pattern"]
            date_format = sweep_config["date_format"]
            start_date = date.fromisoformat(sweep_config["start_date"])
            today = date.today()

            if days:
                start_date = max(start_date, today - timedelta(days=days))

            total_days = (today - start_date).days + 1
            log.info(
                f"{BOLD}DATE SWEEP{RESET} — {source.name} "
                f"{start_date} → {today} ({total_days} days)"
            )

            # Pre-load existing URLs and dead URLs for dedup
            existing_urls = await get_all_source_urls(pool, source.id)
            dead_urls_set = await get_all_dead_urls(pool, source.id)
            log.info(
                f"  {DIM}{source.name}: {len(existing_urls)} articles in DB, "
                f"{len(dead_urls_set)} dead links{RESET}"
            )

            article_patterns = get_article_url_patterns(source.slug)

            # Phase 1: Discover article URLs from daily archive pages
            all_items: list[RSSItem] = []
            seen_urls: set[str] = set(existing_urls) | dead_urls_set

            context = await create_context(browser)
            try:
                page = await context.new_page()
                current = start_date
                empty_streak = 0

                while current <= today:
                    date_str = current.strftime(date_format)
                    archive_url = url_pattern.format(date=date_str)

                    try:
                        await page.goto(archive_url, wait_until="domcontentloaded", timeout=30000)
                        await page.wait_for_timeout(2000)

                        from urllib.parse import urlparse
                        parsed = urlparse(archive_url)
                        base_url = f"{parsed.scheme}://{parsed.netloc}"

                        links = await page.evaluate(
                            _EXTRACT_LINKS_JS,
                            {"baseUrl": base_url, "slug": source.slug, "articleUrlPatterns": article_patterns},
                        )

                        new_count = 0
                        for link in links:
                            url = link["url"]
                            if url not in seen_urls and not _should_skip_url(url):
                                seen_urls.add(url)
                                all_items.append(RSSItem(title=link["title"], link=url))
                                new_count += 1

                        if new_count > 0:
                            log.info(
                                f"  {current.isoformat()}: {new_count} new articles "
                                f"(total: {len(all_items)})"
                            )
                            empty_streak = 0
                        else:
                            empty_streak += 1
                            if empty_streak % 30 == 0:
                                log.info(f"  {DIM}{current.isoformat()}: {empty_streak} consecutive days with no new articles{RESET}")

                    except Exception as e:
                        log.warning(f"  {RED}✗{RESET} {current.isoformat()} failed: {e}")

                    current += timedelta(days=1)

                await page.close()
            finally:
                await context.close()

            if not all_items:
                log.info(f"  {DIM}No new articles found across {total_days} days{RESET}")
                continue

            log.info(f"  {GREEN}✓{RESET} Discovered {len(all_items)} new article URLs")

            # Phase 2: Scrape articles in parallel
            rate_limiter = RateLimiter(settings.rate_limit_ms)
            semaphore = asyncio.Semaphore(concurrency)
            db_lock = asyncio.Lock()
            inserted = 0
            failed = 0
            no_date = 0

            async def _scrape_one(item: RSSItem) -> None:
                nonlocal inserted, failed, no_date
                async with semaphore:
                    await rate_limiter.wait()

                    scraped = await scrape_article_page(browser, item.link, source_slug=source.slug)
                    if isinstance(scraped, ScrapeError):
                        failed += 1
                        await record_dead_link(pool, source.id, scraped.url, scraped.error_type)
                        return
                    if not scraped or not scraped.content or len(scraped.content) < 100:
                        failed += 1
                        return
                    # Successful scrape — remove from dead_links if it was a retry
                    await remove_dead_link(pool, item.link)

                    article_title = scraped.title or normalize_text(item.title)

                    if not scraped.published_at:
                        no_date += 1
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
                        article_id = await insert_article(pool, article)
                        if article_id:
                            inserted += 1
                            if inserted % 10 == 0:
                                log.info(
                                    f"  {GREEN}▸{RESET} Progress: {inserted} inserted, "
                                    f"{failed} failed, {no_date} no date"
                                )

            tasks = [_scrape_one(item) for item in all_items]
            await asyncio.gather(*tasks)

            total_inserted += inserted
            total_skipped += len(all_items) - inserted - failed - no_date

            log.info(
                f"  {GREEN}▸{RESET} {source.name}: {inserted} inserted, "
                f"{failed} failed, {no_date} no date"
            )

    finally:
        if own_browser and browser:
            await browser.close()
            await close_playwright()

    log.info(
        f"{GREEN}▸{RESET} Date sweep complete: {total_inserted} inserted, "
        f"{total_skipped} skipped"
    )
    return {"inserted": total_inserted, "skipped": total_skipped}


async def run_auto_backfill(
    source_slug: str | None = None,
    concurrency: int = 3,
    pages: int | None = None,
    days: int | None = None,
) -> dict:
    """Config-driven backfill — automatically runs the right methods for each source.

    Single-source: runs methods sequentially (archive → nid_sweep → date_sweep).
    Multi-source: archive phase uses intelligent scheduler for interleaved scraping,
    then NID sweep and date sweep run per-source sequentially.
    """
    pool = await get_pool()

    if source_slug:
        source = await get_source_by_slug(pool, source_slug)
        if not source:
            log.error(f"Source not found: {source_slug}")
            return {"error": f"Source not found: {source_slug}"}
        sources = [source]
    else:
        sources = await get_active_sources(pool)

    total_inserted = 0
    total_skipped = 0
    total_not_found = 0

    # Single source → sequential methods (original flow)
    if len(sources) == 1:
        source = sources[0]
        methods = get_backfill_methods(source.slug)
        if not methods:
            log.warning(f"  {YELLOW}–{RESET} No backfill methods for {source.slug}")
            return {"inserted": 0, "skipped": 0, "not_found": 0}

        method_names = [m["type"] for m in methods]
        log.info(
            f"{BOLD}AUTO BACKFILL{RESET} — {source.name}: "
            f"{' → '.join(method_names)}"
        )

        for method in methods:
            result = await _run_single_method(source, method, concurrency, pages, days)
            total_inserted += result.get("inserted", 0)
            total_skipped += result.get("skipped", 0)
            total_not_found += result.get("not_found", 0)

        log.info(
            f"{GREEN}▸{RESET} Auto backfill complete: {total_inserted} inserted, "
            f"{total_skipped} skipped, {total_not_found} not found"
        )
        return {"inserted": total_inserted, "skipped": total_skipped, "not_found": total_not_found}

    # Multi-source → all phases run concurrently with shared browser.
    # Archive uses intelligent scheduler; NID/date sweeps run independently.
    # This prevents slow archive sources (e.g. CF-protected Daily Mirror)
    # from blocking NID/date sweeps for faster sources.

    # Collect methods per source
    archive_sources: list[tuple[Source, int]] = []  # (source, archive_pages)
    nid_sweep_sources: list[Source] = []
    date_sweep_sources: list[tuple[Source, int | None]] = []  # (source, days)

    for source in sources:
        methods = get_backfill_methods(source.slug)
        if not methods:
            continue
        method_names = [m["type"] for m in methods]
        log.info(
            f"{BOLD}AUTO BACKFILL{RESET} — {source.name}: "
            f"{' → '.join(method_names)}"
        )
        for method in methods:
            mt = method["type"]
            if mt == "archive":
                ap = pages or method.get("pages", 5)
                archive_sources.append((source, ap))
            elif mt == "nid_sweep":
                nid_sweep_sources.append(source)
            elif mt == "date_sweep":
                sd = days or method.get("days")
                date_sweep_sources.append((source, sd))

    # Create shared browser for all concurrent phases
    browser = await connect_browser()
    log.info(f"{GREEN}✓{RESET} Playwright connected (shared across all phases)")

    try:
        tasks: list[asyncio.Task] = []
        sweep_concurrency = max(1, min(concurrency, 3))

        # Archive phase (interleaved via scheduler)
        if archive_sources:
            log.info(f"{BOLD}ARCHIVE BACKFILL{RESET} — {len(archive_sources)} sources (interleaved)")
            tasks.append(asyncio.create_task(
                _backfill_archive_interleaved(archive_sources, concurrency, browser=browser)
            ))

        # NID sweeps — all run concurrently with reduced per-source concurrency
        for source in nid_sweep_sources:
            log.info(f"  {DIM}Starting NID sweep for {source.slug} (concurrency={sweep_concurrency})...{RESET}")
            tasks.append(asyncio.create_task(
                run_nid_sweep(source.slug, sweep_concurrency, browser=browser)
            ))

        # Date sweeps — all run concurrently
        for source, sweep_days in date_sweep_sources:
            log.info(f"  {DIM}Starting date sweep for {source.slug} (concurrency={sweep_concurrency})...{RESET}")
            tasks.append(asyncio.create_task(
                run_date_sweep(source.slug, sweep_concurrency, sweep_days, browser=browser)
            ))

        # Run ALL phases concurrently
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate results
        for result in results:
            if isinstance(result, Exception):
                log.error(f"  {RED}✗{RESET} Phase failed: {result}")
                continue
            if isinstance(result, dict):
                total_inserted += result.get("inserted", 0)
                total_skipped += result.get("skipped", 0)
                total_not_found += result.get("not_found", 0)

    finally:
        await browser.close()
        await close_playwright()

    log.info(
        f"{GREEN}▸{RESET} Auto backfill complete: {total_inserted} inserted, "
        f"{total_skipped} skipped, {total_not_found} not found"
    )
    return {"inserted": total_inserted, "skipped": total_skipped, "not_found": total_not_found}


async def _run_single_method(
    source: Source, method: dict, concurrency: int,
    pages: int | None, days: int | None,
) -> dict:
    """Run a single backfill method for one source."""
    method_type = method["type"]

    if method_type == "archive":
        archive_pages = pages or method.get("pages", 5)
        log.info(f"  {DIM}Running archive crawl ({archive_pages} pages)...{RESET}")
        result = await run_backfill(source.slug, archive_pages, concurrency)
    elif method_type == "nid_sweep":
        log.info(f"  {DIM}Running NID sweep...{RESET}")
        result = await run_nid_sweep(source.slug, concurrency)
    elif method_type == "date_sweep":
        sweep_days = days or method.get("days")
        log.info(f"  {DIM}Running date sweep...{RESET}")
        result = await run_date_sweep(source.slug, concurrency, sweep_days)
    else:
        log.warning(f"  {YELLOW}–{RESET} Unknown backfill method: {method_type}")
        return {}

    if "error" in result:
        log.error(f"  {RED}✗{RESET} {method_type} failed: {result['error']}")
    return result


async def _backfill_archive_interleaved(
    archive_sources: list[tuple[Source, int]],
    concurrency: int,
    browser=None,
) -> dict:
    """Run archive backfill across multiple sources with intelligent scheduling.

    Discovers archive URLs from all sources concurrently, then scrapes
    interleaved across sources using the IntelligentScheduler.
    """
    pool = await get_pool()
    own_browser = browser is None
    if own_browser:
        browser = await connect_browser()
        log.info(f"{GREEN}✓{RESET} Playwright connected")

    try:
        scheduler = IntelligentScheduler(browser, pool, global_concurrency=concurrency)

        for source, _ in archive_sources:
            sched = get_scheduling_config(source.slug)
            scheduler.register_source(
                source=source,
                rate_limit_ms=sched["rate_limit_ms"] or settings.rate_limit_ms,
                max_concurrency=sched["max_concurrency"] or concurrency,
                priority=sched["priority"],
            )

        existing_urls: dict[str, set[str]] = {}
        existing_titles: dict[str, set[str]] = {}
        counts: dict[str, dict[str, int]] = {}

        async def _discover_archive(source: Source, archive_pages: int) -> None:
            slug = source.slug
            try:
                discovered = await _crawl_archive_pages(browser, source, archive_pages)
                if not discovered:
                    return

                log.info(f"  {GREEN}✓{RESET} [{slug}] Discovered {len(discovered)} URLs")

                urls = [item.link for item in discovered]
                existing = await get_existing_urls(pool, source.id, urls)
                dead = await get_dead_urls(pool, source.id, urls)
                recent_raw = await get_recent_titles(pool, source.id, days=365)
                titles = {normalize_title(t) for t in recent_raw if len(normalize_title(t)) > 10}

                existing_urls[slug] = existing
                existing_titles[slug] = titles
                counts[slug] = {"inserted": 0, "skipped_no_date": 0, "skipped_duplicate": 0}

                filtered = []
                for item in discovered:
                    if item.link in existing or item.link in dead:
                        continue
                    if _should_skip_url(item.link):
                        continue
                    norm = normalize_title(item.title)
                    if norm and len(norm) > 10 and norm in titles:
                        continue
                    filtered.append(item)

                skipped = len(discovered) - len(filtered)
                if filtered:
                    log.info(f"  [{slug}] {len(filtered)} new articles queued ({skipped} skipped)")
                    await scheduler.enqueue(slug, filtered)
                else:
                    log.info(f"  {DIM}[{slug}] 0 new articles ({skipped} already in DB){RESET}")
            except Exception as e:
                log.error(f"  {RED}✗{RESET} [{slug}] archive discovery failed: {e}")
            finally:
                scheduler.mark_discovery_done(slug)

        # Discover all sources concurrently + start workers immediately
        discovery_tasks = [
            asyncio.create_task(_discover_archive(source, ap))
            for source, ap in archive_sources
        ]
        worker_task = asyncio.create_task(
            scheduler.run(existing_urls, existing_titles, counts)
        )

        await asyncio.gather(*discovery_tasks)
        await worker_task
        await scheduler.cleanup()
    finally:
        if own_browser:
            await browser.close()
            await close_playwright()

    total_inserted = sum(c["inserted"] for c in counts.values())
    total_skipped = sum(c["skipped_duplicate"] + c["skipped_no_date"] for c in counts.values())

    for slug, c in counts.items():
        if c["inserted"] > 0:
            log.info(f"  {GREEN}▸{RESET} {slug}: {c['inserted']} inserted")

    return {"inserted": total_inserted, "skipped": total_skipped}
