"""Pydantic models for structured LLM output."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class QAIssue(BaseModel):
    """A single quality issue found in an article."""

    type: Literal[
        "html_artifact",
        "ad_text",
        "truncated",
        "wrong_language",
        "missing_title",
        "missing_content",
        "encoding_error",
        "duplicate_content",
        "boilerplate",
        "other",
    ]
    severity: Literal["low", "medium", "high"]
    description: str
    suggested_fix: str | None = None


class QAReport(BaseModel):
    """Structured QA review result for a single article."""

    status: Literal["pass", "warn", "fail"]
    issues: list[QAIssue] = Field(default_factory=list)
    content_quality_score: int = Field(ge=1, le=10, description="1=garbage, 10=perfect")
    language_correct: bool
    has_artifacts: bool


class CategoryResult(BaseModel):
    """Structured categorization result for a single article."""

    category: Literal[
        "politics",
        "business",
        "sports",
        "crime",
        "international",
        "opinion",
        "entertainment",
        "health",
        "education",
        "environment",
        "technology",
        "other",
    ]
    entities: list[str] = Field(default_factory=list, description="Key people, organizations, places")
    location: str | None = Field(default=None, description="Where the news event happened")
    summary: str = Field(description="1-2 sentence summary in English")
