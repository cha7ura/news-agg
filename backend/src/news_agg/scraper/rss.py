"""RSS feed parser for Ada Derana English and other RSS sources.

Ported from ground-news/scripts/pipeline.ts lines 193-239 (fetchRSS).

Key quirk: Ada Derana's RSS feed is ISO-8859-1 encoded, not UTF-8.
feedparser handles charset detection automatically, but we use httpx
to fetch first for better control.
"""

from __future__ import annotations

import re
from datetime import datetime

import feedparser
import httpx

from news_agg.models import RSSItem
from news_agg.utils.logging import get_logger

log = get_logger()

_MIN_YEAR = 2025


async def fetch_rss(rss_url: str) -> list[RSSItem]:
    """Fetch and parse RSS feed. Returns list of RSSItem.

    - Handles ISO-8859-1 charset (Ada Derana RSS)
    - Filters articles to MIN_YEAR+
    - Extracts image from description HTML
    """
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(rss_url)
        response.raise_for_status()

    # feedparser handles charset detection from content-type header and XML declaration
    feed = feedparser.parse(response.content)

    items: list[RSSItem] = []
    for entry in feed.entries:
        title = entry.get("title", "Untitled")
        link = entry.get("link") or entry.get("id", "")
        if not link:
            continue

        pub_date = entry.get("published") or entry.get("updated")

        # Filter old articles
        if pub_date:
            try:
                from email.utils import parsedate_to_datetime

                dt = parsedate_to_datetime(pub_date)
                if dt.year < _MIN_YEAR:
                    continue
            except Exception:
                pass

        # Extract description
        description = entry.get("summary") or entry.get("description")

        # Extract image from description HTML (pipeline.ts lines 221-224)
        image_url: str | None = None
        if description:
            img_match = re.search(r'src=[\'\"](https?://[^\'\"]+)[\'"]', description, re.IGNORECASE)
            if img_match:
                image_url = img_match.group(1)

        items.append(
            RSSItem(
                title=title,
                link=link,
                pub_date=pub_date,
                description=description,
                image_url=image_url,
            )
        )

    return items
