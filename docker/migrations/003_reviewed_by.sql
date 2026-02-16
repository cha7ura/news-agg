-- 003: Track which LLM model reviewed each article
-- Apply with: psql -U newsagg -d newsagg -f docker/migrations/003_reviewed_by.sql

ALTER TABLE articles
  ADD COLUMN IF NOT EXISTS reviewed_by TEXT DEFAULT NULL;
