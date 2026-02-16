"""LangChain chain definitions for article QA review and categorization."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

from news_agg.agents.models import CategoryResult, QAReport
from news_agg.config import settings

_PROMPTS_DIR = Path(__file__).parent / "prompts"


def _get_llm() -> ChatOpenAI:
    """Create OpenRouter-backed LLM instance."""
    return ChatOpenAI(
        model=settings.openrouter_model,
        api_key=settings.openrouter_api_key,
        base_url=settings.llm_base_url,
        temperature=0,
        max_tokens=8192,
    )


def _load_prompt(filename: str) -> ChatPromptTemplate:
    """Load a prompt template from YAML file."""
    path = _PROMPTS_DIR / filename
    with open(path) as f:
        data = yaml.safe_load(f)

    return ChatPromptTemplate.from_messages([
        ("system", data["system"]),
    ])


def _build_json_prompt(base_prompt: ChatPromptTemplate, schema_class: type) -> ChatPromptTemplate:
    """Append a human message with the article data and JSON schema instruction."""
    schema = schema_class.model_json_schema()
    # Escape curly braces so LangChain doesn't treat JSON schema as template vars
    schema_str = json.dumps(schema, indent=2).replace("{", "{{").replace("}", "}}")

    return base_prompt + ChatPromptTemplate.from_messages([
        ("human",
         "Review this article and respond with ONLY valid JSON matching this schema:\n"
         f"```json\n{schema_str}\n```\n\n"
         "Article:\n"
         "- Source: {source}\n"
         "- Language: {language}\n"
         "- Title: {title}\n"
         "- Author: {author}\n"
         "- Published: {published_at}\n"
         "- Content (first 2000 chars):\n{content}\n"),
    ])


def build_qa_chain(prompt_version: str = "v1"):
    """Build QA review chain: article → QAReport."""
    base = _load_prompt(f"qa_review_{prompt_version}.yaml")
    prompt = _build_json_prompt(base, QAReport)
    llm = _get_llm()

    # Try structured output first; fall back to raw text parsing
    try:
        return prompt | llm.with_structured_output(QAReport)
    except Exception:
        return prompt | llm


def build_categorize_chain(prompt_version: str = "v1"):
    """Build categorization chain: article → CategoryResult."""
    base = _load_prompt(f"categorize_{prompt_version}.yaml")
    prompt = _build_json_prompt(base, CategoryResult)
    llm = _get_llm()

    try:
        return prompt | llm.with_structured_output(CategoryResult)
    except Exception:
        return prompt | llm
