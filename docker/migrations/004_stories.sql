-- Phase 1A: Story clustering — groups articles about the same event
-- Applied by: news-agg db-migrate

-- Stories table: clusters of articles covering the same news event
CREATE TABLE IF NOT EXISTS stories (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT NOT NULL,
    summary TEXT,
    category TEXT,
    entities TEXT[],
    location TEXT,
    image_url TEXT,
    article_count INT DEFAULT 1,
    source_count INT DEFAULT 1,
    first_published_at TIMESTAMPTZ,
    last_updated_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Link articles to stories
ALTER TABLE articles ADD COLUMN IF NOT EXISTS story_id UUID REFERENCES stories(id);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_articles_story ON articles(story_id);
CREATE INDEX IF NOT EXISTS idx_articles_no_story ON articles(id) WHERE story_id IS NULL;
CREATE INDEX IF NOT EXISTS idx_stories_updated ON stories(last_updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_stories_category ON stories(category);
CREATE INDEX IF NOT EXISTS idx_stories_first_published ON stories(first_published_at DESC);
