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

# Test archive backfill
uv run news-agg ingest --source example-news-en --backfill --pages 2 --concurrency 3

# Test NID sweep (if configured)
uv run news-agg ingest --source example-news-en --nid-sweep --concurrency 3

# Check article counts
uv run news-agg check
```

## 6. Full Ingest

Once testing looks good, run a full ingest:

```bash
# Archive backfill (discover URLs from paginated archive pages)
uv run news-agg ingest --source example-news-en --backfill --pages 40 --concurrency 5

# NID sweep (iterate through all article IDs — most thorough)
uv run news-agg ingest --source example-news-en --nid-sweep --concurrency 5
```

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
| `docker/init.sql` | Database schema + seed sources |
