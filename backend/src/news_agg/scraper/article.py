"""Article page content extraction using Playwright.

Ported from ground-news/scripts/pipeline.ts lines 305-480 (scrapeArticlePage).
Extracts title, content, date, author, image via CSS selector cascades,
then applies a 5-level date extraction waterfall.
"""

from __future__ import annotations

import re
from datetime import datetime

from playwright.async_api import Browser, BrowserContext

from news_agg.models import ScrapedArticle
from news_agg.scraper.browser import create_context
from news_agg.text.dates import extract_date_waterfall
from news_agg.text.normalize import normalize_text
from news_agg.utils.logging import get_logger

log = get_logger()

# Default CSS selectors — work for most WordPress/news sites
# Ported from pipeline.ts lines 96-116
_DEFAULT_SELECTORS = {
    "title": [
        "h1.entry-title", "h1.article-title", "h1.post-title",
        "article h1", ".article-header h1", "h1",
    ],
    "author": [
        ".author-name", ".byline", ".article-author", ".writer-name",
        '[rel="author"]', ".post-author",
    ],
    "date": [
        "time[datetime]", ".publish-date", ".article-date",
        ".post-date", ".entry-date", ".date", ".news-datestamp",
    ],
    "content": [
        # Ada Derana Sinhala specific
        ".news-content", "#DivNewsContent", ".story-body",
        # Generic news selectors
        "article .entry-content", ".article-body", ".article-content",
        ".story-text", ".inner-content", ".entry-content", ".post-content",
        ".content-area", "#article-body", ".inner-fontstyle", "article", "main .content",
    ],
    "image": [
        ".article-image img", "article img", ".featured-image img",
    ],
}

# Meta tags to check for dates (pipeline.ts lines 119-122)
_DATE_META_TAGS = [
    "article:published_time", "og:article:published_time",
    "datePublished", "publishedTime", "dateModified", "modifiedTime",
]

_AUTHOR_META_TAGS = ["author", "article:author"]

# Byline/dateline patterns to strip from article content
# Matches: "By Author Name\n", "By Author Colombo,...", "By D.G. Sugathapala"
_BYLINE_RE = re.compile(
    r"^By\s+([A-Za-z][A-Za-z. ]+?)(?:\s*\n|(?=\s+Colombo|\s+[A-Z]{2,}))",
    re.MULTILINE,
)
# Dateline pattern A: "Colombo, Feb. 13 (Daily Mirror) -" / "Colombo, 14th February (DailyMirror) -"
_DATELINE_COLOMBO_RE = re.compile(
    r"^Colombo,?\s+.{0,60}?\((?:Daily\s?Mirror|DailyMirror|Mirror\s+Sports)\)\s*-?\s*",
    re.IGNORECASE,
)
# Dateline pattern B: "Feb.14 (Mirror Sports) -" / "Feb.13 -" (no Colombo prefix)
_DATELINE_SHORT_RE = re.compile(
    r"^(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s*"
    r"\d{1,2}(?:st|nd|rd|th)?"
    r"(?:\s*\((?:Daily\s?Mirror|DailyMirror|Mirror\s+Sports)\))?"
    r"\s*-\s*",
    re.IGNORECASE,
)
# Dateline pattern C: "COLOMBO (News 1st);" / "COLOMBO (News1st):"
_DATELINE_NEWS1ST_RE = re.compile(
    r"^COLOMBO\s*\(News\s?1st\)\s*[;:–-]\s*",
    re.IGNORECASE,
)

# Single page.evaluate() call to extract everything at once
# Ported from pipeline.ts lines 336-424
_EXTRACT_JS = """
(params) => {
    const { sel, dateMetaTags, authorMetaTags } = params;

    function trySelectors(selectors) {
        for (const css of selectors) {
            if (css.startsWith('meta[')) {
                const el = document.querySelector(css);
                if (el?.content?.trim()) return el.content.trim();
                continue;
            }
            const el = document.querySelector(css);
            if (el && el.textContent && el.textContent.trim().length > 0) {
                return el.textContent.trim();
            }
        }
        return '';
    }

    function trySelectorsAttr(selectors, attr) {
        for (const css of selectors) {
            const el = document.querySelector(css);
            if (el) {
                const val = el.getAttribute(attr);
                if (val) return val.trim();
            }
        }
        return '';
    }

    // Collect all meta tags
    const metas = {};
    document.querySelectorAll('meta').forEach((m) => {
        const name = m.getAttribute('property') || m.getAttribute('name') || '';
        const content = m.getAttribute('content') || '';
        if (name && content) metas[name] = content;
    });

    // Title
    const title = trySelectors(sel.title) || metas['og:title'] || '';

    // Author
    let author = '';
    for (const key of authorMetaTags) {
        if (metas[key]) { author = metas[key]; break; }
    }
    if (!author) author = trySelectors(sel.author);

    // Daily Mirror: author embedded in content as <p><em><strong>By Author</strong></em></p>
    if (!author) {
        const contentEl = document.querySelector('.a-content, .article-body, .entry-content, article');
        if (contentEl) {
            const paras = contentEl.querySelectorAll('p');
            for (let i = 0; i < Math.min(paras.length, 5); i++) {
                const txt = paras[i].textContent?.trim();
                if (txt) {
                    const m = txt.match(/^By\\s+([A-Z][A-Za-z. ]+?)(?:\\s*$|\\s+(?:Colombo|[A-Z]{2,}))/);
                    if (m) { author = m[1].trim(); break; }
                }
            }
        }
    }

    // Date
    let dateStr = '';
    for (const key of dateMetaTags) {
        if (metas[key]) { dateStr = metas[key]; break; }
    }
    if (!dateStr) dateStr = trySelectorsAttr(sel.date, 'datetime');
    if (!dateStr) dateStr = trySelectors(sel.date);

    // Daily Mirror: date is in <a> with hidden <span>Published :</span>
    if (!dateStr) {
        const pubLinks = document.querySelectorAll('a.text-decoration-none');
        for (const a of pubLinks) {
            const span = a.querySelector('span');
            if (span && /Published/i.test(span.textContent)) {
                dateStr = a.textContent.replace(/Published\\s*:\\s*/i, '').trim();
                break;
            }
        }
    }

    // Content — clone element and strip scripts/styles/ads before reading text
    function cleanTextContent(el) {
        const clone = el.cloneNode(true);
        clone.querySelectorAll('script, style, noscript, iframe, nav, header, footer, aside, .navbar, .navigation, .menu, .google-auto-placed, .adsbygoogle, [id*="google_ads"], [class*="social"], .share-buttons, .comments-section, #aiSummaryBox, .ai-quickread-box, .ai-quickread, .ai-qr-title, .ai-qr-subtitle, .ai-quickread-badge, .ai-quickread-hide, .ai-quickread-loading, .fotorama, figure figcaption').forEach(n => n.remove());
        return clone.textContent?.trim() || '';
    }

    let content = '';
    for (const css of sel.content) {
        const el = document.querySelector(css);
        if (el) {
            const cleaned = cleanTextContent(el);
            if (cleaned.length > 200) {
                content = cleaned;
                break;
            }
        }
    }
    if (!content) content = cleanTextContent(document.body) || '';

    // Image
    const imageUrl = metas['og:image']
        || trySelectorsAttr(sel.image, 'src')
        || (document.querySelector('article img'))?.src
        || '';

    // Body text for date fallback — use article area first to avoid nav/ad text
    // consuming the char budget. Strip scripts/styles before reading textContent.
    const articleArea = document.querySelector('.news_body_areas, .news-content, article, main');
    const bodySource = articleArea || document.body;
    const bodyClone = bodySource.cloneNode(true);
    bodyClone.querySelectorAll('script, style, noscript, nav, header, footer, aside, .google-auto-placed, .adsbygoogle').forEach(n => n.remove());
    const bodyText = bodyClone.textContent?.trim().slice(0, 3000) || '';

    return { title, author, dateStr, content, imageUrl, bodyText };
}
"""


_EXCERPT_SKIP_RE = re.compile(
    r"^(By\s+[A-Z]|Photo\s*:|Pic\s*:|Image\s*:|Courtesy\s*:|Colombo,?\s"
    r"|COLOMBO\s*\("
    r"|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s*\d)",
    re.IGNORECASE,
)


def _extract_excerpt(content: str, max_len: int = 300) -> str | None:
    """Extract first meaningful paragraph for use as excerpt.

    Skips bylines, credits, and short lines.
    """
    for line in content.split("\n"):
        trimmed = line.strip()
        if not trimmed or trimmed.startswith("#") or trimmed.startswith("![") or trimmed.startswith("---"):
            continue
        if len(trimmed) < 40:
            continue
        if _EXCERPT_SKIP_RE.match(trimmed):
            continue
        return trimmed[:max_len]
    return content[:max_len] if content else None


async def scrape_article_page(
    browser_or_ctx: Browser | BrowserContext,
    url: str,
    rss_pub_date: str | None = None,
    source_slug: str | None = None,
) -> ScrapedArticle | None:
    """Scrape a single article page. Returns None if content too short or scrape fails.

    Accepts either a Browser (creates a fresh context per page — needed for
    Cloudflare-protected sites) or a BrowserContext (reuses existing context).
    Uses per-source CSS selectors from sources.yaml when source_slug is provided,
    falling back to default selectors. Applies a 5-level date waterfall for dates.
    """
    # Load per-source selectors if available
    if source_slug:
        from news_agg.source_config import get_date_meta_tags, get_selectors
        selectors = get_selectors(source_slug)
        date_meta = get_date_meta_tags(source_slug)
    else:
        selectors = _DEFAULT_SELECTORS
        date_meta = _DATE_META_TAGS

    # Determine whether to create a fresh context or reuse existing one
    own_context = isinstance(browser_or_ctx, Browser)
    context: BrowserContext | None = None
    page = None
    try:
        if own_context:
            context = await create_context(browser_or_ctx)
            page = await context.new_page()
        else:
            page = await browser_or_ctx.new_page()

        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(2000)

        # Detect Cloudflare challenge ("Just a moment...") and wait for it to resolve
        title = await page.title()
        if "just a moment" in title.lower():
            log.info(f"  Cloudflare challenge detected, waiting...")
            for _ in range(10):
                await page.wait_for_timeout(1000)
                title = await page.title()
                if "just a moment" not in title.lower():
                    break
            else:
                log.warning(f"  Cloudflare challenge did not resolve for {url}")
                return None

        # Extract everything in one page.evaluate() call
        # Capture the final URL after any redirects (e.g. news.php?nid=123 → /news/123/slug)
        final_url = page.url

        result = await page.evaluate(
            _EXTRACT_JS,
            {
                "sel": selectors,
                "dateMetaTags": date_meta,
                "authorMetaTags": _AUTHOR_META_TAGS,
            },
        )

        # Content minimum check (pipeline.ts line 429)
        if not result["content"] or len(result["content"]) < 100:
            return None

        # Date extraction waterfall (pipeline.ts lines 432-467)
        published_at: datetime | None = extract_date_waterfall(
            meta_date=result["dateStr"] or None,
            selector_date=result["dateStr"] or None,
            url=url,
            body_text=result["bodyText"],
            rss_pub_date=rss_pub_date,
        )

        # Normalize text
        content = normalize_text(result["content"])
        title = normalize_text(result["title"]) if result["title"] else ""
        author = normalize_text(result["author"]) if result["author"] else None

        # Clean author: strip "by " prefix and trailing date artifacts
        # (e.g. NewsFirst ".author_main" returns "by Zulfick Farzan 14-02-2026 | 3:44 AM")
        if author:
            author = re.sub(r'^[Bb]y\s+', '', author)
            author = re.sub(r'\s*\d{1,2}[-/]\d{1,2}[-/]\d{4}.*$', '', author).strip()
            if not author:
                author = None

        # Strip byline/dateline from content
        m_byline = _BYLINE_RE.match(content)
        if m_byline:
            if not author:
                author = m_byline.group(1).strip()
            content = content[m_byline.end():]
        for pat in (_DATELINE_COLOMBO_RE, _DATELINE_SHORT_RE, _DATELINE_NEWS1ST_RE):
            m_dateline = pat.match(content)
            if m_dateline:
                content = content[m_dateline.end():]

        excerpt = _extract_excerpt(content)

        return ScrapedArticle(
            title=title,
            content=content,
            author=author,
            published_at=published_at,
            image_url=result["imageUrl"] or None,
            excerpt=excerpt,
            final_url=final_url,
        )
    except Exception as e:
        log.warning(f"Scrape failed for {url}: {e}")
        return None
    finally:
        if page:
            try:
                await page.close()
            except Exception:
                pass
        if own_context and context:
            try:
                await context.close()
            except Exception:
                pass
