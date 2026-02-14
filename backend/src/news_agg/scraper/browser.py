"""Playwright browser lifecycle — connects to Docker Playwright server via WebSocket.

Ported from ground-news/scripts/pipeline.ts lines 956-963 (browser connect).
The Playwright server runs as a Docker service, and we connect over WebSocket.
"""

from __future__ import annotations

from playwright.async_api import Browser, BrowserContext, Playwright, async_playwright

from news_agg.config import settings
from news_agg.utils.logging import get_logger

log = get_logger()

# Keep the playwright instance alive for the session
_playwright: Playwright | None = None


async def connect_browser() -> Browser:
    """Connect to the Playwright Docker service via WebSocket.

    The Docker service runs `npx playwright run-server --port 3000`
    and we connect to it at ws://localhost:3100 (mapped port).
    """
    global _playwright
    _playwright = await async_playwright().start()
    browser = await _playwright.chromium.connect(settings.playwright_ws_url, timeout=15000)
    return browser


async def create_context(browser: Browser) -> BrowserContext:
    """Create a browser context with Chrome-like user agent and optional proxy.

    One context per source — shared across all article pages for that source.
    Proxy (e.g. Tor SOCKS5) is applied here so all page navigations go through it.
    Realistic viewport/locale/timezone settings prevent Cloudflare bot detection.
    """
    kwargs: dict = {
        "user_agent": settings.user_agent,
        "viewport": {"width": 1920, "height": 1080},
        "locale": "en-US",
        "timezone_id": "Asia/Colombo",
    }
    if settings.proxy_url:
        kwargs["proxy"] = {"server": settings.proxy_url}
    return await browser.new_context(**kwargs)


async def close_playwright() -> None:
    """Clean up the playwright instance."""
    global _playwright
    if _playwright:
        await _playwright.stop()
        _playwright = None
