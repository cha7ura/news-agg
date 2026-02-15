# Article QA Review Pipeline — Design

**Date**: 2026-02-16
**Status**: Approved
**Goal**: Post-ingestion quality assurance using LLM agents via OpenRouter

## Problem

With 23 sources and 73K+ articles, scrape quality varies. Some articles have HTML artifacts, ad text, truncated content, wrong language tags, or missing metadata. Manual spot-checking doesn't scale. We need an automated QA pipeline that samples articles and produces structured quality reports.

## Architecture

```
CLI: uv run news-agg review --sample N [--source X] [--since DATE]
         │
         ▼
    Sample articles from DB (random or filtered)
         │
         ▼
    Agent 1: QA Reviewer (LangChain + OpenRouter)
    - Check content quality (artifacts, truncation, ads)
    - Verify language matches source config
    - Flag missing/weak metadata
    - Output: QAReport (Pydantic structured output)
         │ (only passes)
         ▼
    Agent 2: Categorizer
    - Assign news category
    - Extract entities (people, orgs, locations)
    - Generate 1-2 sentence summary
    - Output: CategoryResult (Pydantic structured output)
         │
         ▼
    Print report to console + optionally save to article_reviews table
```

## Data Models

```python
class QAIssue(BaseModel):
    type: Literal["html_artifact", "ad_text", "truncated", "wrong_language",
                   "missing_title", "missing_date", "missing_content",
                   "encoding_error", "duplicate_content", "other"]
    severity: Literal["low", "medium", "high"]
    description: str
    suggested_fix: str | None = None

class QAReport(BaseModel):
    status: Literal["pass", "warn", "fail"]
    issues: list[QAIssue]
    content_quality_score: int  # 1-10
    language_correct: bool
    has_artifacts: bool

class CategoryResult(BaseModel):
    category: Literal["politics", "business", "sports", "crime",
                       "international", "opinion", "entertainment",
                       "health", "education", "environment", "other"]
    entities: list[str]  # key people, organizations
    location: str | None
    summary: str  # 1-2 sentences
```

## Prompt Versioning

Prompts stored as YAML in `backend/src/news_agg/agents/prompts/`:

```yaml
# qa_review_v1.yaml
version: v1
model: null  # use default from .env
description: "Basic QA review for article content quality"
system: |
  You are a news article quality reviewer...
human: |
  Review this article for quality issues:
  Title: {title}
  Content: {content}
  Language: {language}
  Source: {source}
  ...
```

Active version configurable via CLI `--prompt-version v1` or defaults to latest.

## CLI Interface

```bash
uv run news-agg review --sample 10              # random 10 articles
uv run news-agg review --source ft-en --sample 20
uv run news-agg review --since 2026-02-15
uv run news-agg review --categorize-only --sample 10
uv run news-agg review --prompt-version v2
```

## Tech Stack

- **LangChain** (`langchain-openai`): OpenRouter integration via ChatOpenAI
- **Pydantic**: Structured output models (already a dependency)
- **OpenRouter**: LLM provider (free tier: nvidia/nemotron-3-nano-30b-a3b)
- **asyncpg**: DB access (existing)
- **Click**: CLI (existing)

## File Structure

```
backend/src/news_agg/agents/
  __init__.py
  chains.py           # LangChain chain definitions
  models.py           # Pydantic QAReport, CategoryResult
  prompts/
    qa_review_v1.yaml
    categorize_v1.yaml
  runner.py           # Orchestrator: sample → review → categorize → report
```

## DB Schema (optional, for persisting reviews)

```sql
CREATE TABLE IF NOT EXISTS article_reviews (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    article_id UUID NOT NULL REFERENCES articles(id) ON DELETE CASCADE,
    qa_status TEXT NOT NULL,  -- pass/warn/fail
    qa_score INT,
    qa_issues JSONB,
    category TEXT,
    entities JSONB,
    location TEXT,
    summary TEXT,
    prompt_version TEXT,
    model_used TEXT,
    reviewed_at TIMESTAMPTZ DEFAULT NOW()
);
```

## Phase 2 (Future): Inline Review

Once prompts are tuned via batch review, add optional inline review during ingestion:
- New flag: `uv run news-agg ingest --review`
- Articles pass through QA chain before DB insert
- Failed articles logged but still inserted (with `qa_status = 'fail'`)

## Success Criteria

1. `uv run news-agg review --sample 10` produces a readable QA report
2. Each article gets a structured pass/warn/fail status with specific issues
3. Passing articles get categorized with entities and summary
4. Prompt versions are swappable without code changes
5. Works with the free-tier OpenRouter model (graceful fallback on bad output)
