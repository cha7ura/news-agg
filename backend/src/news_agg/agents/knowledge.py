"""Graphiti knowledge graph integration for article storage.

Adds articles that pass QA review to a Neo4j-backed knowledge graph
using Graphiti for automatic entity/relationship extraction.
"""

from __future__ import annotations

from datetime import datetime, timezone

from news_agg.config import settings
from news_agg.utils.logging import get_logger, GREEN, YELLOW, RED, DIM, RESET

log = get_logger()

_graphiti_client = None


def _make_embedder():
    """Create a local sentence-transformers embedder subclassing Graphiti's EmbedderClient."""
    from collections.abc import Iterable as _Iterable

    from graphiti_core.embedder.client import EmbedderClient

    class _SentenceTransformerEmbedder(EmbedderClient):
        """Uses all-MiniLM-L6-v2 (384-dim) — downloads on first use (~80MB)."""

        def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
            from sentence_transformers import SentenceTransformer

            log.info(f"  {DIM}Loading embedding model: {model_name}{RESET}")
            self._model = SentenceTransformer(model_name)

        async def create(
            self, input_data: str | list[str] | _Iterable[int] | _Iterable[_Iterable[int]]
        ) -> list[float]:
            if isinstance(input_data, str):
                embedding = self._model.encode(input_data)
            else:
                texts = list(input_data)
                embedding = self._model.encode(texts[0] if texts else "")
            return embedding.tolist()

        async def create_batch(self, input_data_list: list[str]) -> list[list[float]]:
            embeddings = self._model.encode(input_data_list)
            return [e.tolist() for e in embeddings]

    return _SentenceTransformerEmbedder()


async def get_graphiti_client():
    """Initialize Graphiti client with OpenRouter LLM and local embeddings.

    Returns None if Neo4j is not configured or connection fails.
    The review pipeline continues without the knowledge graph in either case.
    """
    global _graphiti_client

    if _graphiti_client is not None:
        return _graphiti_client

    if not settings.neo4j_password:
        log.info(f"  {DIM}Neo4j not configured (skipping knowledge graph){RESET}")
        return None

    try:
        from graphiti_core import Graphiti
        from graphiti_core.llm_client.openai_generic_client import OpenAIGenericClient
        from graphiti_core.llm_client.config import LLMConfig
        from graphiti_core.cross_encoder.openai_reranker_client import OpenAIRerankerClient

        # LLM client: OpenAI-compatible API (OpenRouter, Ollama, LM Studio)
        llm_config = LLMConfig(
            api_key=settings.active_api_key,
            model=settings.active_model,
            small_model=settings.active_model,
            base_url=settings.llm_base_url,
        )
        llm_client = OpenAIGenericClient(config=llm_config)

        # Embedder: local sentence-transformers (OpenRouter doesn't serve embeddings)
        embedder = _make_embedder()

        # Cross-encoder: reuse LLM client for reranking
        cross_encoder = OpenAIRerankerClient(client=llm_client, config=llm_config)

        _graphiti_client = Graphiti(
            settings.neo4j_uri,
            settings.neo4j_user,
            settings.neo4j_password,
            llm_client=llm_client,
            embedder=embedder,
            cross_encoder=cross_encoder,
        )

        await _graphiti_client.build_indices_and_constraints()
        log.info(f"  {GREEN}Graphiti connected to Neo4j{RESET}")
        return _graphiti_client

    except Exception as e:
        log.warning(f"  {YELLOW}Graphiti init failed: {e}{RESET}")
        log.info(f"  {DIM}Continuing without knowledge graph{RESET}")
        return None


async def close_graphiti_client() -> None:
    """Close Graphiti client and Neo4j connection."""
    global _graphiti_client
    if _graphiti_client:
        try:
            await _graphiti_client.close()
        except Exception:
            pass
        finally:
            _graphiti_client = None


async def add_article_to_graph(article: dict, category_result) -> bool:
    """Add a QA-passed article to the knowledge graph as an episode.

    Graphiti automatically extracts entities and relationships from the
    article text. The category and source slug are stored as metadata.

    Returns True if added successfully, False otherwise.
    """
    client = await get_graphiti_client()
    if not client:
        return False

    try:
        from graphiti_core.nodes import EpisodeType

        title = article.get("title", "") or ""
        content = article.get("content", "") or ""
        source_slug = article.get("source_slug", "unknown")

        # Build episode body
        episode_body = f"{title}\n\n{content[:3000]}"

        # Build source description with category + location
        source_desc = f"{source_slug} - {category_result.category}"
        if category_result.location:
            source_desc += f" ({category_result.location})"

        # Parse reference time
        published_at = article.get("published_at")
        if isinstance(published_at, datetime):
            reference_time = published_at
        elif published_at:
            reference_time = datetime.fromisoformat(str(published_at))
        else:
            reference_time = datetime.now(timezone.utc)

        await client.add_episode(
            name=article.get("url", f"article-{article.get('id')}"),
            episode_body=episode_body,
            source=EpisodeType.text,
            source_description=source_desc,
            reference_time=reference_time,
            group_id=source_slug,
        )

        log.info(
            f"    {GREEN}+graph{RESET} "
            f"{DIM}entities={category_result.entities[:3]}{RESET}"
        )
        return True

    except Exception as e:
        log.error(f"    {RED}graph failed: {e}{RESET}")
        return False
