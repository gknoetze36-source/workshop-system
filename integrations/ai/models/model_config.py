from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ModelRoute:
    """The single-provider Phase 10 route.

    Phase 10 intentionally uses OpenAI only.  The provider abstraction remains
    so a second provider can be added in a later phase without changing PHANTA
    business logic, but no fallback provider is configured or attempted now.
    """

    provider: str
    model: str
    input_cost_per_million: float = 0.0
    output_cost_per_million: float = 0.0


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _model(task_env: str) -> str:
    return _env(task_env, _env("PHANTA_AI_MODEL", "gpt-5"))


# Provider is deliberately fixed to OpenAI for Phase 10.
# Model names remain environment-configurable so a supported GPT model can be
# selected without changing application code.
MODEL_CONFIG: dict[str, ModelRoute] = {
    "conversation": ModelRoute(
        provider="openai",
        model=_model("PHANTA_AI_CONVERSATION_MODEL"),
        input_cost_per_million=float(_env("PHANTA_AI_CONVERSATION_INPUT_COST_PER_M", "0")),
        output_cost_per_million=float(_env("PHANTA_AI_CONVERSATION_OUTPUT_COST_PER_M", "0")),
    ),
    "summarization": ModelRoute(
        provider="openai",
        model=_model("PHANTA_AI_SUMMARY_MODEL"),
        input_cost_per_million=float(_env("PHANTA_AI_SUMMARY_INPUT_COST_PER_M", "0")),
        output_cost_per_million=float(_env("PHANTA_AI_SUMMARY_OUTPUT_COST_PER_M", "0")),
    ),
    "extraction": ModelRoute(
        provider="openai",
        model=_model("PHANTA_AI_EXTRACTION_MODEL"),
        input_cost_per_million=float(_env("PHANTA_AI_EXTRACTION_INPUT_COST_PER_M", "0")),
        output_cost_per_million=float(_env("PHANTA_AI_EXTRACTION_OUTPUT_COST_PER_M", "0")),
    ),
}


def get_route(task_type: str) -> ModelRoute:
    try:
        return MODEL_CONFIG[task_type]
    except KeyError as exc:
        raise ValueError(f"No AI model route configured for task_type={task_type!r}") from exc
