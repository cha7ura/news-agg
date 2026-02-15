"""Intelligent multi-source scheduler with autoscaling.

Interleaves scraping across multiple sources so workers never idle on
per-source rate limits. Each source has its own rate limiter and
concurrency cap; workers pull from whichever source is ready next.

Autoscaling: monitors queue depth and error rate every 3 seconds.
If the queue is growing and errors are low, spawns more workers
(up to max_workers=25). If errors spike, reduces workers.
"""

from __future__ import annotations

import asyncio
import logging
import time

import asyncpg
from playwright.async_api import Browser, BrowserContext

from news_agg.config import settings
from news_agg.db import insert_article, record_dead_link, remove_dead_link
from news_agg.models import ArticleCreate, RSSItem, ScrapeError, Source
from news_agg.scraper.article import scrape_article_page
from news_agg.scraper.browser import create_context
from news_agg.text.dedup import normalize_title
from news_agg.text.normalize import normalize_text
from news_agg.utils.logging import GREEN, RED, YELLOW, DIM, RESET, get_logger
from news_agg.utils.rate_limit import RateLimiter

log = get_logger()


class SourceState:
    """Per-source mutable state: queue, rate limiter, concurrency tracking."""

    def __init__(
        self,
        source: Source,
        rate_limit_ms: int,
        max_concurrency: int,
        priority: int,
    ):
        self.source = source
        self.rate_limiter = RateLimiter(rate_limit_ms)
        self.max_concurrency = max_concurrency
        self.priority = priority
        self.queue: asyncio.Queue[RSSItem] = asyncio.Queue()
        self.active_count = 0
        self.discovery_done = False
        self.items_scraped = 0
        self.errors = 0

        # CF sources need a fresh BrowserContext per page; others share one
        self.needs_fresh_ctx: bool = not source.rss_url
        self.shared_context: BrowserContext | None = None
        self._ctx_lock = asyncio.Lock()


class IntelligentScheduler:
    """Feeds URLs to workers, interleaving sources to eliminate idle time.

    Usage:
        scheduler = IntelligentScheduler(browser, pool, global_concurrency=5)
        scheduler.register_source(source, rate_limit_ms=500, ...)
        await scheduler.enqueue("slug", items)
        scheduler.mark_discovery_done("slug")
        result = await scheduler.run(existing_urls, existing_titles)
    """

    MAX_WORKERS = 25
    AUTOSCALE_INTERVAL = 3.0  # seconds between autoscale checks
    SCALE_UP_STEP = 2         # add this many workers per scale-up
    ERROR_RATE_BACKOFF = 0.3  # error rate threshold to trigger scale-down

    def __init__(
        self,
        browser: Browser,
        pool: asyncpg.Pool,
        global_concurrency: int = 5,
    ):
        self.browser = browser
        self.pool = pool
        self.initial_concurrency = global_concurrency
        self.sources: dict[str, SourceState] = {}
        self.db_lock = asyncio.Lock()
        self._pick_lock = asyncio.Lock()
        # Autoscaling state
        self._worker_tasks: list[asyncio.Task] = []
        self._stop_event = asyncio.Event()
        self._last_queue_depth = 0
        self._total_scraped = 0
        self._total_errors = 0

    def register_source(
        self,
        source: Source,
        rate_limit_ms: int,
        max_concurrency: int,
        priority: int,
    ) -> None:
        self.sources[source.slug] = SourceState(
            source, rate_limit_ms, max_concurrency, priority,
        )

    async def enqueue(self, slug: str, items: list[RSSItem]) -> None:
        state = self.sources[slug]
        for item in items:
            await state.queue.put(item)

    def mark_discovery_done(self, slug: str) -> None:
        self.sources[slug].discovery_done = True

    # ------------------------------------------------------------------
    # Scheduling core
    # ------------------------------------------------------------------

    async def _pick_next(self) -> tuple[SourceState, RSSItem] | None:
        """Pick the next task from any source that's ready.

        Priority: ready sources first (sorted by priority, then fairness),
        then wait for shortest cooldown. Returns None when all work is done.
        """
        while True:
            sleep_time: float | None = None

            async with self._pick_lock:
                all_done = True
                candidates: list[tuple[SourceState, float]] = []

                for state in self.sources.values():
                    if not state.queue.empty():
                        all_done = False
                        if state.active_count < state.max_concurrency:
                            wait_time = state.rate_limiter.time_until_ready()
                            candidates.append((state, wait_time))
                    elif not state.discovery_done:
                        all_done = False

                if all_done:
                    return None

                if not candidates:
                    # All sources either empty (waiting for discovery) or at
                    # their per-source concurrency cap — sleep outside lock
                    sleep_time = 0.05
                else:
                    ready = [(s, w) for s, w in candidates if w <= 0]
                    if ready:
                        # Sort by priority (lower=higher), then fairness
                        ready.sort(key=lambda x: (x[0].priority, x[0].items_scraped))
                        state = ready[0][0]
                    else:
                        # Nothing ready — capture shortest cooldown, sleep outside lock
                        candidates.sort(key=lambda x: x[1])
                        sleep_time = candidates[0][1]

                    if sleep_time is None:
                        # Claim rate limit slot and dequeue
                        await state.rate_limiter.wait()
                        try:
                            item = state.queue.get_nowait()
                            state.active_count += 1
                            return (state, item)
                        except asyncio.QueueEmpty:
                            continue

            # Sleep outside the lock so other workers aren't blocked
            if sleep_time is not None:
                await asyncio.sleep(sleep_time)

    # ------------------------------------------------------------------
    # Worker pool with autoscaling
    # ------------------------------------------------------------------

    def _queue_depth(self) -> int:
        return sum(s.queue.qsize() for s in self.sources.values())

    def _active_workers(self) -> int:
        return sum(1 for t in self._worker_tasks if not t.done())

    async def run(
        self,
        existing_urls: dict[str, set[str]],
        existing_titles: dict[str, set[str]],
        counts: dict[str, dict[str, int]],
    ) -> None:
        """Launch worker pool with autoscaling that drains all source queues."""

        async def _worker() -> None:
            while not self._stop_event.is_set():
                result = await self._pick_next()
                if result is None:
                    return
                state, item = result
                try:
                    await self._scrape_and_insert(
                        state, item, existing_urls, existing_titles, counts,
                    )
                finally:
                    state.active_count -= 1
                    state.items_scraped += 1

        # Start initial workers
        for _ in range(self.initial_concurrency):
            self._worker_tasks.append(asyncio.create_task(_worker()))
        log.info(f"  {DIM}Autoscale: started {self.initial_concurrency} workers (max {self.MAX_WORKERS}){RESET}")

        # Start autoscaler alongside workers
        autoscaler = asyncio.create_task(self._autoscaler(_worker))

        # Wait for all workers to finish
        while True:
            alive = [t for t in self._worker_tasks if not t.done()]
            if not alive:
                break
            await asyncio.gather(*alive, return_exceptions=True)

        self._stop_event.set()
        autoscaler.cancel()
        try:
            await autoscaler
        except asyncio.CancelledError:
            pass

    async def _autoscaler(self, worker_fn) -> None:
        """Monitor queue depth and error rate, scale workers up or down."""
        await asyncio.sleep(self.AUTOSCALE_INTERVAL)  # initial grace period

        while not self._stop_event.is_set():
            queue_depth = self._queue_depth()
            active = self._active_workers()
            total_scraped = sum(s.items_scraped for s in self.sources.values())
            total_errors = sum(s.errors for s in self.sources.values())

            # Calculate error rate over recent window
            recent_total = total_scraped - self._total_scraped
            recent_errors = total_errors - self._total_errors
            error_rate = recent_errors / max(recent_total, 1)

            self._total_scraped = total_scraped
            self._total_errors = total_errors

            if error_rate >= self.ERROR_RATE_BACKOFF and active > 1:
                # High errors — kill some workers by letting them exit naturally
                # (they check _stop_event but we don't set it — we just don't add more)
                target = max(1, active // 2)
                removed = active - target
                log.info(
                    f"  {YELLOW}↓{RESET} Autoscale: errors {error_rate:.0%} — "
                    f"reducing to {target} workers (-{removed})"
                )
                # Cancel the newest workers to scale down
                for task in reversed(self._worker_tasks):
                    if not task.done() and active > target:
                        task.cancel()
                        active -= 1

            elif queue_depth > active * 2 and active < self.MAX_WORKERS:
                # Queue growing, headroom available — scale up
                add = min(self.SCALE_UP_STEP, self.MAX_WORKERS - active)
                if add > 0:
                    for _ in range(add):
                        self._worker_tasks.append(asyncio.create_task(worker_fn()))
                    log.info(
                        f"  {GREEN}↑{RESET} Autoscale: queue={queue_depth} — "
                        f"scaled to {active + add} workers (+{add})"
                    )

            # Prune completed tasks to avoid unbounded list growth
            self._worker_tasks = [t for t in self._worker_tasks if not t.done()]

            await asyncio.sleep(self.AUTOSCALE_INTERVAL)

    async def _scrape_and_insert(
        self,
        state: SourceState,
        item: RSSItem,
        existing_urls: dict[str, set[str]],
        existing_titles: dict[str, set[str]],
        counts: dict[str, dict[str, int]],
    ) -> None:
        """Scrape one article and insert into DB."""
        source = state.source
        slug = source.slug

        # Determine scraper target (CF vs shared context)
        if state.needs_fresh_ctx:
            scraper_target: Browser | BrowserContext = self.browser
        else:
            async with state._ctx_lock:
                if state.shared_context is None:
                    state.shared_context = await create_context(self.browser)
            scraper_target = state.shared_context

        scraped = await scrape_article_page(
            scraper_target, item.link, item.pub_date, slug,
        )

        if isinstance(scraped, ScrapeError):
            state.errors += 1
            log.warning(f"  {RED}✗{RESET} [{slug}] {item.title[:50]}... ({scraped.error_type})")
            await record_dead_link(self.pool, source.id, scraped.url, scraped.error_type)
            return

        if not scraped or not scraped.content or len(scraped.content) < 100:
            state.errors += 1
            log.warning(f"  {RED}✗{RESET} [{slug}] {item.title[:50]}... (scrape failed or too short)")
            return

        await remove_dead_link(self.pool, item.link)
        article_title = scraped.title or normalize_text(item.title)

        if not scraped.published_at:
            counts[slug]["skipped_no_date"] += 1
            log.warning(f"  {YELLOW}–{RESET} [{slug}] {article_title[:50]}... (NO DATE)")
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

        async with self.db_lock:
            norm_title = normalize_title(item.title)
            source_titles = existing_titles.get(slug, set())
            if norm_title and len(norm_title) > 10 and norm_title in source_titles:
                counts[slug]["skipped_duplicate"] += 1
                return

            article_id = await insert_article(self.pool, article)
            if article_id:
                log.info(
                    f"  {GREEN}✓{RESET} [{slug}] {article_title[:50]}... "
                    f"({len(scraped.content)} chars)"
                )
                counts[slug]["inserted"] += 1
                source_titles.add(norm_title)
                existing_urls.get(slug, set()).add(item.link)
            else:
                counts[slug]["skipped_duplicate"] += 1

    async def cleanup(self) -> None:
        """Close all shared browser contexts."""
        for state in self.sources.values():
            if state.shared_context:
                try:
                    await state.shared_context.close()
                except Exception:
                    pass
