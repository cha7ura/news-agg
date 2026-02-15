-- news-agg Phase 1 schema

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Sources table: news outlets we scrape from
CREATE TABLE IF NOT EXISTS sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    slug TEXT UNIQUE NOT NULL,
    url TEXT NOT NULL,
    rss_url TEXT,
    language TEXT DEFAULT 'en',
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Articles table: individual scraped articles
CREATE TABLE IF NOT EXISTS articles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    url TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    content TEXT,
    excerpt TEXT,
    image_url TEXT,
    author TEXT,
    published_at TIMESTAMPTZ,
    scraped_at TIMESTAMPTZ DEFAULT NOW(),
    language TEXT DEFAULT 'en',
    original_language TEXT DEFAULT 'en',
    is_processed BOOLEAN DEFAULT false,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source_id);
CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_language ON articles(language);
CREATE INDEX IF NOT EXISTS idx_articles_created ON articles(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_is_processed ON articles(is_processed) WHERE NOT is_processed;

-- Seed: news sources
INSERT INTO sources (name, slug, url, rss_url, language) VALUES
    ('Ada Derana', 'ada-derana-en', 'https://www.adaderana.lk', 'https://www.adaderana.lk/rss.php', 'en'),
    ('Ada Derana Sinhala', 'ada-derana-si', 'https://sinhala.adaderana.lk', NULL, 'si'),
    ('Daily Mirror', 'daily-mirror-en', 'https://www.dailymirror.lk', NULL, 'en'),
    ('NewsFirst', 'newsfirst-en', 'https://english.newsfirst.lk', NULL, 'en'),
    ('The Island', 'island-en', 'https://island.lk', 'https://island.lk/feed/', 'en'),
    ('EconomyNext', 'economynext-en', 'https://economynext.com', 'https://economynext.com/feed/', 'en'),
    ('Colombo Gazette', 'colombo-gazette-en', 'https://colombogazette.com', NULL, 'en'),
    ('News19', 'news19-si', 'https://www.news19.lk', 'https://www.news19.lk/feed/', 'si')
ON CONFLICT (slug) DO NOTHING;

-- Dead links: track failed scrape URLs with graduated retry
CREATE TABLE IF NOT EXISTS dead_links (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_id UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    url TEXT UNIQUE NOT NULL,
    error_type TEXT NOT NULL,     -- '404', 'timeout', '500', 'cloudflare', 'empty', 'unknown'
    first_failed_at TIMESTAMPTZ DEFAULT NOW(),
    last_checked_at TIMESTAMPTZ DEFAULT NOW(),
    retry_count INTEGER DEFAULT 0, -- 0→7d, 1→14d, 2→30d, 3→permanent
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dead_links_source ON dead_links(source_id);
CREATE INDEX IF NOT EXISTS idx_dead_links_url ON dead_links(url);
