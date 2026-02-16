-- 002: Add QA tracking columns to articles + agent run history table
-- Apply with: psql -U newsagg -d newsagg -f docker/migrations/002_agent_schema.sql

-- QA tracking on articles
ALTER TABLE articles
  ADD COLUMN IF NOT EXISTS qa_status TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS qa_score INTEGER DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS qa_issues JSONB DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS category TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS entities TEXT[] DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS location TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS summary TEXT DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ DEFAULT NULL,
  ADD COLUMN IF NOT EXISTS graph_saved BOOLEAN DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_articles_unreviewed ON articles(id) WHERE qa_status IS NULL;
CREATE INDEX IF NOT EXISTS idx_articles_qa_status ON articles(qa_status);
CREATE INDEX IF NOT EXISTS idx_articles_graph_unsaved ON articles(id) WHERE qa_status = 'pass' AND graph_saved = false;

-- Agent run history
CREATE TABLE IF NOT EXISTS agent_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    thread_id TEXT NOT NULL,
    config JSONB DEFAULT '{}',
    result JSONB DEFAULT '{}',
    decisions JSONB DEFAULT '[]',
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ DEFAULT NULL,
    error_message TEXT DEFAULT NULL
);

CREATE INDEX IF NOT EXISTS idx_agent_runs_status ON agent_runs(status);
CREATE INDEX IF NOT EXISTS idx_agent_runs_started ON agent_runs(started_at DESC);
