# Adding a New News Source

Step-by-step guide for adding a new source to the news aggregation pipeline.

## 1. Discover URL Patterns

Use Firecrawl (or manually browse) to map the site's structure:

```bash
# Map all URLs on the site
firecrawl map https://example-news.com -o .firecrawl/example-urls.txt

# Search for archive/pagination patterns
firecrawl map https://example-news.com --search "page" -o .firecrawl/example-pages.txt

# Scrape the homepage to see link structure
firecrawl scrape https://example-news.com --format links -o .firecrawl/example-links.json
```

What to look for:
- **Article URL pattern**: e.g. `/news/12345/article-slug`, `/article?id=999`
- **Section URLs**: e.g. `/sports/`, `/business/`, `/entertainment/`
- **Archive pagination**: e.g. `?pageno=N`, `?page=N`, `/page/N`
- **RSS feed**: Usually at `/rss`, `/feed`, `/rss.xml`
- **Sequential article IDs (nid)**: If articles use numeric IDs, note the range

## 2. Add Source to Database

Add a row to the `sources` table (via pgAdmin at http://localhost:5050 or SQL):

```sql
INSERT INTO sources (name, slug, url, rss_url, language) VALUES
  ('Example News', 'example-news-en', 'https://example-news.com', 'https://example-news.com/rss.xml', 'en');
```

**Fields:**
- `slug`: Unique identifier, used in CLI commands and config (e.g. `example-news-en`)
- `rss_url`: Set to `NULL` if no RSS feed (will use listing page fallback)
- `language`: `en`, `si`, `ta`, etc.

## 3. Add Source Config to `sources.yaml`

Edit `backend/src/news_agg/sources.yaml`:

```yaml
example-news-en:
  name: Example News
  url: https://example-news.com
  rss_url: https://example-news.com/rss.xml  # or null
  language: en

  # Sections with listing and archive pages
  sections:
    news:
      listing_url: https://example-news.com/latest/
      archive_pattern: "https://example-news.com/latest/?page={page}"
      max_pages: 40
    sports:
      listing_url: https://example-news.com/sports/
      archive_pattern: "https://example-news.com/sports/?page={page}"
      max_pages: 20

  # Regex patterns that identify valid article URLs
  article_url_patterns:
    - '/news/\d+/'
    - '/sports/\d+/'

  # URL patterns to skip (non-article pages)
  skip_url_patterns:
    - '/(category|tag|author|page)/'
    - '\.(jpg|png|pdf)$'

  # CSS selectors for content extraction (tried in order, first match wins)
  selectors:
    title:
      - h1.article-title
      - article h1
      - h1
    content:
      - .article-body
      - .entry-content
      - article
    date:
      - time[datetime]
      - .publish-date
    author:
      - .author-name
      - .byline
    image:
      - .article-image img
      - article img

  # NID sweep config (optional — for sequential article ID iteration)
  nid_sweep:
    - url_pattern: "https://example-news.com/article/{nid}"
      start: 1000
      end: 50000
      max_consecutive_404: 50

  # Meta tags for date extraction
  date_meta_tags:
    - article:published_time
    - og:article:published_time
    - datePublished

  # Backfill config — declares which methods work for this source
  # Methods run in order: archive first (fast), then nid_sweep (thorough)
  backfill:
    methods:
      - type: archive      # Crawl paginated archive pages
        pages: 40           # Number of pages to crawl
      - type: nid_sweep     # Iterate through sequential article IDs
      # - type: date_sweep  # For date-based archive pages (e.g. /YYYY/MM/DD)
      #   days: null        # null = full range from start_date

  # Scheduling config — per-source tuning for the intelligent queue
  # Optional: sources without this block get defaults (500ms, global concurrency, priority 5)
  scheduling:
    rate_limit_ms: 500       # Min delay between requests to this source (default: 500)
    max_concurrency: 5       # Max concurrent scrapers for this source (default: --concurrency)
    priority: 0              # Lower = higher priority (default: 5)
```

## 4. Find CSS Selectors

Open an article page in the browser and use DevTools (F12) to identify selectors:

1. **Title**: Usually `h1` — check for specific classes like `h1.news-heading`
2. **Content**: The article body container — look for `.article-body`, `.entry-content`, etc.
3. **Date**: Look for `<time datetime="...">` tags or `.publish-date` elements
4. **Author**: Check for `.byline`, `.author-name`, or `[rel="author"]`
5. **Image**: The main article image, often `.featured-image img`

You can also scrape a sample article to inspect:

```bash
firecrawl scrape "https://example-news.com/news/12345/some-article" --html -o .firecrawl/sample-article.html
```

## 5. Test the Source

```bash
# Test regular ingest (RSS or listing page)
uv run news-agg ingest --source example-news-en --limit 5

# Test auto-backfill (runs configured methods in order: archive → nid_sweep)
uv run news-agg ingest --source example-news-en --backfill --pages 2 --concurrency 3

# Override: run a specific method directly
uv run news-agg ingest --source example-news-en --nid-sweep --concurrency 3
uv run news-agg ingest --source example-news-en --date-sweep --concurrency 3

# Check article counts
uv run news-agg check
```

## 6. Full Ingest

Once testing looks good, run a full backfill. The `--backfill` flag automatically runs
all configured methods for the source (declared in `backfill.methods`):

```bash
# Auto-backfill — runs archive crawl then NID sweep (or whatever's configured)
uv run news-agg ingest --source example-news-en --backfill --concurrency 5

# Override archive page count (default comes from config)
uv run news-agg ingest --source example-news-en --backfill --pages 100 --concurrency 5
```

## Multi-Source Intelligent Scheduling

When running without `--source` (all sources), the pipeline uses an intelligent queue
that interleaves scraping across sources. Workers pull from whichever source's rate limit
has cooled down — with 11 sources and 500ms per-source limits, workers effectively never wait.

```bash
# Multi-source ingest — interleaved, ~3-5x faster than sequential
uv run news-agg ingest --limit 20 --concurrency 5

# Multi-source backfill — archive phase interleaved, NID/date sweep sequential
uv run news-agg ingest --backfill --pages 5 --concurrency 5
```

**Per-source tuning** via `scheduling:` in sources.yaml:
- `rate_limit_ms`: Cloudflare sources need slower rates (e.g. 1000ms for Daily Mirror)
- `max_concurrency`: CF sources need fewer concurrent requests (e.g. 2)
- `priority`: Lower number = scraped first (0 = highest, default = 5)

**Autoscaling:** The worker pool automatically adjusts based on load:
- Starts with `--concurrency N` workers
- Scales up by 2 workers when queue depth exceeds 2× active workers (and errors are low)
- Scales down by half when error rate exceeds 30% (protects against CF blocks)
- Hard cap at 25 workers maximum
- Check interval: every 3 seconds

## Troubleshooting

| Problem | Fix |
|---------|-----|
| **0 articles found** | Check `article_url_patterns` — the listing page JS filter might exclude valid URLs |
| **Scrape fails (None)** | Check CSS selectors — open article in DevTools and verify selectors match |
| **No date extracted** | Add source-specific `date_meta_tags` or date CSS selectors |
| **Duplicate articles** | Check if URL format varies (e.g. `http` vs `https`, with/without trailing slash) |
| **Content has ads/scripts** | The `cleanTextContent()` JS strips common ad elements — add source-specific exclusions if needed |
| **Archive pages empty** | Verify `archive_pattern` — try the URL manually in a browser |

## File Reference

| File | Purpose |
|------|---------|
| `sources.yaml` | Per-source config (selectors, URL patterns, archive pages) |
| `source_config.py` | YAML config loader functions |
| `scraper/article.py` | Article content extraction (CSS selectors + JS) |
| `scraper/listing.py` | Listing page link discovery (JS) |
| `scraper/rss.py` | RSS feed parser |
| `pipeline.py` | Regular ingest orchestrator |
| `backfill.py` | Archive backfill + NID sweep |
| `scheduler.py` | Intelligent multi-source queue scheduler |
| `docker/init.sql` | Database schema + seed sources |
