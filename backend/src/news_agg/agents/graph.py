"""LangGraph agent for the agentic news pipeline.

Uses create_react_agent with tool-calling to orchestrate:
ingest → review → hydrate → graph-save cycles.

Checkpointed to PostgreSQL for durable execution and run history.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import yaml
from langchain_core.messages import SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from news_agg.agents.tools import ALL_TOOLS
from news_agg.config import settings
from news_agg.db import create_agent_run, get_pool
from news_agg.utils.logging import get_logger, BOLD, DIM, GREEN, RED, RESET

log = get_logger()


def _load_system_prompt() -> str:
    """Load the orchestrator system prompt from YAML."""
    prompt_path = Path(__file__).parent / "prompts" / "orchestrator_v1.yaml"
    with open(prompt_path) as f:
        data = yaml.safe_load(f)
    return data["system_prompt"]


def _build_llm() -> ChatOpenAI:
    """Build the LLM client using OpenRouter."""
    return ChatOpenAI(
        model=settings.openrouter_model,
        api_key=settings.openrouter_api_key,
        base_url="https://openrouter.ai/api/v1",
        temperature=0.1,
    )


async def _build_checkpointer():
    """Build an async Postgres checkpointer, or fall back to in-memory."""
    try:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

        checkpointer = AsyncPostgresSaver.from_conn_string(settings.database_url)
        await checkpointer.setup()
        log.info(f"  {GREEN}✓{RESET} Postgres checkpointer ready")
        return checkpointer
    except Exception as e:
        log.warning(f"  {DIM}Postgres checkpointer failed ({e}), using in-memory{RESET}")
        from langgraph.checkpoint.memory import InMemorySaver

        return InMemorySaver()


async def build_agent():
    """Build and return the compiled LangGraph agent."""
    llm = _build_llm()
    system_prompt = _load_system_prompt()
    checkpointer = await _build_checkpointer()

    agent = create_react_agent(
        model=llm,
        tools=ALL_TOOLS,
        checkpointer=checkpointer,
        prompt=system_prompt,
    )
    return agent, checkpointer


async def run_agent_cycle(
    sources: list[str] | None = None,
    limit: int = 20,
    run_type: str = "full_cycle",
) -> dict:
    """Execute a full agent pipeline cycle.

    The agent autonomously decides what to ingest, review, and save based on
    current pipeline status and run history. Returns the final result summary.

    Args:
        sources: Optional list of source slugs to focus on.
        limit: Article limit per source for ingestion.
        run_type: Type of run ('full_cycle', 'ingest_only', 'review_only').
    """
    pool = await get_pool()
    thread_id = str(uuid.uuid4())

    # Record run start
    run_id = await create_agent_run(pool, run_type, thread_id, {
        "sources": sources,
        "limit": limit,
    })

    log.info(f"{BOLD}AGENT{RESET} — starting {run_type} (run={run_id})")
    log.info(f"  {DIM}thread={thread_id}{RESET}")

    agent, checkpointer = await build_agent()
    config = {"configurable": {"thread_id": thread_id}}

    # Build the initial user message instructing the agent
    parts = [f"Run ID: {run_id}", f"Run type: {run_type}"]
    if sources:
        parts.append(f"Focus sources: {', '.join(sources)}")
    parts.append(f"Article limit per source: {limit}")
    parts.append("Begin the pipeline cycle now.")

    user_message = "\n".join(parts)

    try:
        result = await agent.ainvoke(
            {"messages": [{"role": "user", "content": user_message}]},
            config=config,
        )

        # Extract final message
        final_msg = result["messages"][-1].content if result.get("messages") else "No response"
        log.info(f"\n{BOLD}Agent completed:{RESET}")
        log.info(f"  {DIM}{final_msg[:500]}{RESET}")

        return {
            "run_id": str(run_id),
            "thread_id": thread_id,
            "status": "completed",
            "summary": final_msg,
        }

    except Exception as e:
        log.error(f"{RED}Agent failed: {e}{RESET}")
        from news_agg.db import update_agent_run

        await update_agent_run(pool, run_id, "failed", error_message=str(e))
        return {
            "run_id": str(run_id),
            "thread_id": thread_id,
            "status": "failed",
            "error": str(e),
        }
    finally:
        # Clean up checkpointer if it has a close method
        if hasattr(checkpointer, "__aexit__"):
            try:
                await checkpointer.__aexit__(None, None, None)
            except Exception:
                pass
