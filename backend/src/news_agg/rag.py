"""RAG pipeline — conversational search over news articles.

Retrieves relevant articles via Meilisearch, then uses an LLM to
generate a cited answer. Session state is kept in-memory for now.

Flow:
    1. User query → Meilisearch full-text search (top-10)
    2. Build context prompt with article excerpts
    3. LLM generates answer with inline source citations [1], [2]...
    4. Return answer + source list + session_id
"""

from __future__ import annotations

import uuid
from collections import OrderedDict

from openai import AsyncOpenAI

from news_agg.config import settings
from news_agg.search import search_articles
from news_agg.utils.logging import get_logger

log = get_logger()

# In-memory session store with LRU eviction (max 200 sessions)
MAX_SESSIONS = 200
_sessions: OrderedDict[str, list[dict]] = OrderedDict()

SYSTEM_PROMPT = """\
You are a Sri Lankan news analyst. Answer questions using ONLY the provided news articles.

Rules:
- Cite sources using [1], [2] etc. matching the article numbers below.
- If articles don't contain enough info, say so honestly.
- Be concise but thorough. Prefer bullet points for multi-part answers.
- When sources disagree, highlight the differences.
- Never fabricate information not in the articles.
- Respond in the same language as the question."""

MAX_CONTEXT_CHARS = 6000


def _build_context(hits: list[dict]) -> tuple[str, list[dict]]:
    """Build context string from Meilisearch hits and return source list."""
    sources = []
    context_parts = []

    for i, hit in enumerate(hits, 1):
        title = hit.get("title") or "Untitled"
        source_name = hit.get("source_name") or hit.get("source_slug", "unknown")
        excerpt = hit.get("excerpt") or (hit.get("content") or "")[:400]
        published = hit.get("published_at") or ""
        url = hit.get("url") or ""

        sources.append({
            "title": title,
            "url": url,
            "source": source_name,
            "published_at": published,
        })

        part = f"[{i}] {title} ({source_name}, {published[:10] if published else 'n/d'})\n{excerpt}\n"
        context_parts.append(part)

    # Truncate context to fit token budget
    context = ""
    for part in context_parts:
        if len(context) + len(part) > MAX_CONTEXT_CHARS:
            break
        context += part + "\n"

    return context, sources


def _get_client() -> AsyncOpenAI:
    """Get OpenAI-compatible client for LLM calls."""
    return AsyncOpenAI(
        base_url=settings.llm_base_url,
        api_key=settings.active_api_key,
    )


async def ask(
    query: str,
    session_id: str | None = None,
    limit: int = 10,
) -> dict:
    """Conversational RAG: search articles, generate cited answer.

    Returns: {answer, sources, session_id, articles_searched}
    """
    # Generate or reuse session ID
    sid = session_id or str(uuid.uuid4())

    # 1. Retrieve articles from Meilisearch
    try:
        result = search_articles(query, limit=limit)
        hits = result.get("hits", [])
    except Exception as e:
        log.warning("Meilisearch search failed: %s", e)
        hits = []

    if not hits:
        return {
            "answer": "I couldn't find any relevant articles for your question. Try rephrasing or broadening your search.",
            "sources": [],
            "session_id": sid,
            "articles_searched": 0,
        }

    # 2. Build context from retrieved articles
    context, sources = _build_context(hits)

    # 3. Build conversation messages
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]

    # Add prior conversation turns for context
    if sid in _sessions:
        _sessions.move_to_end(sid)  # Mark as recently used
    for turn in _sessions.get(sid, [])[-6:]:  # Keep last 3 Q&A pairs
        messages.append(turn)

    # Add current turn with article context
    user_msg = f"Articles:\n{context}\n\nQuestion: {query}"
    messages.append({"role": "user", "content": user_msg})

    # 4. Call LLM
    try:
        client = _get_client()
        response = await client.chat.completions.create(
            model=settings.active_model,
            messages=messages,
            temperature=0.3,
            max_tokens=1024,
        )
        answer = response.choices[0].message.content or "No response generated."
    except Exception as e:
        log.error("LLM call failed: %s", e)
        answer = f"I found {len(hits)} relevant articles but couldn't generate a summary. Error: {str(e)[:100]}"

    # 5. Save to session history (with LRU eviction)
    if sid not in _sessions:
        _sessions[sid] = []
    _sessions[sid].append({"role": "user", "content": query})
    _sessions[sid].append({"role": "assistant", "content": answer})
    _sessions.move_to_end(sid)
    while len(_sessions) > MAX_SESSIONS:
        _sessions.popitem(last=False)  # Evict oldest session

    return {
        "answer": answer,
        "sources": sources,
        "session_id": sid,
        "articles_searched": len(hits),
    }
