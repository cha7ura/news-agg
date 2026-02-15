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
    ('News19', 'news19-si', 'https://www.news19.lk', 'https://www.news19.lk/feed/', 'si'),
    ('Sunday Observer', 'sunday-observer-en', 'https://www.sundayobserver.lk', 'https://www.sundayobserver.lk/feed/', 'en'),
    ('Lanka News Web', 'lanka-news-web-en', 'https://lankanewsweb.net', 'https://lankanewsweb.net/feed/', 'en'),
    ('Lanka Truth', 'lankatruth-si', 'https://lankatruth.com/si', NULL, 'si'),
    -- Tier 1
    ('Hiru News', 'hiru-news-en', 'https://hirunews.lk', NULL, 'en'),
    ('Daily FT', 'ft-en', 'https://www.ft.lk', NULL, 'en'),
    ('Lankadeepa', 'lankadeepa-si', 'https://www.lankadeepa.lk', NULL, 'si'),
    ('Divaina', 'divaina-si', 'https://www.divaina.lk', NULL, 'si'),
    -- Tier 2
    ('Colombo Telegraph', 'colombo-telegraph-en', 'https://www.colombotelegraph.com', NULL, 'en'),
    ('Daily News', 'daily-news-en', 'https://www.dailynews.lk', NULL, 'en'),
    ('Dinamina', 'dinamina-si', 'https://www.dinamina.lk', NULL, 'si'),
    -- Tier 3
    ('Deshaya', 'deshaya-si', 'https://deshaya.lk', NULL, 'si'),
    ('NewsNow', 'newsnow-si', 'https://www.newsnow.lk', 'https://www.newsnow.lk/feed/', 'si'),
    ('Lakresa', 'lakresa-si', 'https://lakresa.net', 'https://lakresa.net/feed/', 'si'),
    ('News.lk', 'news-lk-en', 'https://news.lk', NULL, 'en'),
    ('Ada.lk', 'ada-si', 'https://www.ada.lk', NULL, 'si')
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
