"""LangGraph state schema for the agentic pipeline."""

from __future__ import annotations

import operator
from typing import Annotated, TypedDict


class PipelineState(TypedDict):
    """State passed between graph nodes.

    `messages` accumulates tool-use and LLM response messages.
    Other fields track per-stage results for the final report.
    """

    messages: Annotated[list, operator.add]
    run_id: str
    run_type: str
    ingest_results: dict
    review_results: dict
    graph_results: dict
    current_stage: str
    errors: Annotated[list[str], operator.add]
