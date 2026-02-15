"""Langfuse Cloud integration for LLM observability."""

from __future__ import annotations

import os

from news_agg.config import settings
from news_agg.utils.logging import get_logger, YELLOW, DIM, RESET

log = get_logger()


def get_langfuse_handler():
    """Initialize Langfuse callback handler for LangChain tracing.

    Returns None if Langfuse is not configured or initialization fails.
    The review pipeline continues without tracing in either case.
    """
    if not settings.langfuse_public_key or not settings.langfuse_secret_key:
        log.info(f"  {DIM}Langfuse not configured (skipping tracing){RESET}")
        return None

    try:
        from langfuse.langchain import CallbackHandler

        # Langfuse v3 reads config from env vars
        os.environ.setdefault("LANGFUSE_PUBLIC_KEY", settings.langfuse_public_key)
        os.environ.setdefault("LANGFUSE_SECRET_KEY", settings.langfuse_secret_key)
        os.environ.setdefault("LANGFUSE_HOST", settings.langfuse_base_url)

        handler = CallbackHandler()
        log.info(f"  {DIM}Langfuse tracing enabled{RESET}")
        return handler
    except Exception as e:
        log.warning(f"  {YELLOW}Langfuse init failed: {e}{RESET}")
        return None
