"""Async rate limiter for polite scraping.

Ensures minimum delay between requests to the same domain.
Default: 2000ms (matching ground-news/scripts/pipeline.ts line 1180).

Uses asyncio.Lock to be safe when called from concurrent tasks.
"""

import asyncio
import time


class RateLimiter:
    def __init__(self, delay_ms: int = 2000):
        self._delay = delay_ms / 1000.0
        self._last_request = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self._lock:
            elapsed = time.monotonic() - self._last_request
            if elapsed < self._delay:
                await asyncio.sleep(self._delay - elapsed)
            self._last_request = time.monotonic()
