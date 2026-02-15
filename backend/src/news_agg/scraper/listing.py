"""Listing page scraper — discovers article links from a source's homepage.

Used as fallback when RSS is broken/empty (e.g., Ada Derana Sinhala).
Ported from ground-news/scripts/pipeline.ts lines 507-587 (scrapeListingPage).
"""

from __future__ import annotations

from playwright.async_api import Browser

from news_agg.scraper.browser import create_context
from news_agg.source_config import get_article_url_patterns, get_listing_urls
from news_agg.models import RSSItem
from news_agg.utils.logging import get_logger

log = get_logger()

# Source-specific listing page URLs (fallback when sources.yaml has no sections)
_LISTING_URLS: dict[str, str] = {
    "ada-derana-si": "https://sinhala.adaderana.lk/",
    "ada-derana-en": "https://www.adaderana.lk/hot-news/",
}

# JavaScript executed in the browser to extract article links
# Ported from pipeline.ts lines 532-570
_EXTRACT_LINKS_JS = """
(params) => {
    const anchors = Array.from(document.querySelectorAll('a[href]'));
    const articleLinks = [];
    const seen = new Set();

    for (const a of anchors) {
        let href = a.href;
        const text = a.textContent?.trim() || '';

        // Must be same domain and have reasonable title
        if (!href.startsWith(params.baseUrl) && !href.startsWith('/')) continue;
        if (text.length < 10 || text.length > 300) continue;
        if (seen.has(href)) continue;

        // Skip navigation/category/media links
        if (/\\/(category|tag|page|author|wp-content|feed|login)\\//i.test(href)) continue;
        if (/\\.(jpg|jpeg|png|gif|svg|webp|pdf)$/i.test(href)) continue;

        // Strip URL fragments (#comments, #respond, etc.) for dedup
        href = href.split('#')[0];
        if (seen.has(href)) continue;

        // For Ada Derana, only keep news/sports URLs
        if (params.slug.startsWith('ada-derana')) {
            if (!/\\/news[\\/.]/.test(href) && !/\\/sports[\\/.]/.test(href)) continue;
            // Normalize protocol and strip query params/anchors for dedup
            href = href.replace(/^http:/, 'https:').split('?')[0].split('#')[0];
            // Remove trailing slash for consistent dedup
            if (href.endsWith('/')) href = href.slice(0, -1);
        } else {
            // Apply article URL pattern filter if provided
            // Match against pathname + search (supports query-string URLs like ?p=123)
            let matchedByPattern = false;
            if (params.articleUrlPatterns && params.articleUrlPatterns.length > 0) {
                try {
                    const url = new URL(href);
                    const fullPath = url.pathname + url.search;
                    matchedByPattern = params.articleUrlPatterns.some(p => new RegExp(p).test(fullPath));
                    if (!matchedByPattern) continue;
                } catch { continue; }
            }

            // Check URL path length (at least 3 path segments after domain)
            // Skip this check if the URL already matched an article pattern
            if (!matchedByPattern) {
                try {
                    const url = new URL(href);
                    const segments = url.pathname.split('/').filter(Boolean);
                    if (segments.length < 3) continue;
                } catch { continue; }
            }
        }

        // Skip generic link text
        if (/^(වැඩි විස්තර|more|comments|\\(\\d+\\)|read more)/i.test(text)) continue;

        if (seen.has(href)) continue;
        seen.add(href);
        articleLinks.push({ url: href, title: text });
    }
    return articleLinks;
}
"""


async def scrape_listing_page(
    browser: Browser,
    source_url: str,
    source_slug: str,
    limit: int,
) -> list[RSSItem]:
    """Extract article links from a source's listing page using Playwright.

    Uses listing URLs from sources.yaml sections when available, falling back to
    hardcoded URLs and then the source homepage. Creates a context with realistic
    browser settings to avoid Cloudflare bot detection.

    Returns a list of RSSItem (pub_date will be None — dates come from article scraping).
    """
    # Get listing URLs: sources.yaml sections → hardcoded → source homepage
    listing_urls = get_listing_urls(source_slug)
    if not listing_urls:
        listing_urls = [_LISTING_URLS.get(source_slug, source_url)]

    # Get article URL patterns for filtering
    article_patterns = get_article_url_patterns(source_slug)

    context = None
    try:
        context = await create_context(browser)
        all_items: list[RSSItem] = []
        seen_urls: set[str] = set()

        for listing_url in listing_urls:
            if len(all_items) >= limit:
                break

            page = await context.new_page()
            try:
                await page.goto(listing_url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(3000)

                # Detect Cloudflare challenge and wait
                title = await page.title()
                if "just a moment" in title.lower():
                    log.info(f"  Cloudflare challenge on listing page, waiting...")
                    for _ in range(10):
                        await page.wait_for_timeout(1000)
                        title = await page.title()
                        if "just a moment" not in title.lower():
                            break
                    else:
                        log.warning(f"  Cloudflare did not resolve for {listing_url}")
                        continue

                # Extract article links via browser-side JavaScript
                base_url = f"{listing_url.split('//')[0]}//{listing_url.split('//')[1].split('/')[0]}"
                links = await page.evaluate(
                    _EXTRACT_LINKS_JS,
                    {
                        "baseUrl": base_url,
                        "slug": source_slug,
                        "articleUrlPatterns": article_patterns,
                    },
                )

                for link in links:
                    if link["url"] not in seen_urls:
                        seen_urls.add(link["url"])
                        all_items.append(RSSItem(title=link["title"], link=link["url"]))
            finally:
                await page.close()

        await context.close()
        context = None

        return all_items[:limit]
    except Exception as e:
        log.error(f"Listing page scrape failed for {source_slug}: {e}")
        if context:
            await context.close()
        return []
