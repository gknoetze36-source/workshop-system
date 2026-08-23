# PHANTA Phase 4 — AI Platform Integration Layer

## Scope

Phase 4 implements the integration-layer contract described in the PHANTA Integrations Blueprint: provider abstraction, static model routing, normalized tool/structured-output requests, usage logging, retry/fallback behavior, rate limiting, prompt versioning, and deterministic output guarding.

The Service Advisor remains the business-facing consumer. Provider SDK details do not leak into Service Advisor code.

## Providers

- Anthropic — primary provider by default.
- OpenAI — fallback provider by default.
- Gemini — supported adapter for future routing.

API keys are environment variables only:

- `ANTHROPIC_API_KEY`
- `OPENAI_API_KEY`
- `GEMINI_API_KEY`

## Routing

`integrations/ai/models/model_config.py` contains task-type routes. Model names can be changed with environment variables without changing application code.

## Usage and observability

Every dispatcher call records provider, model, task type, request ID, token usage, latency, success/failure and estimated cost through `AIUsageRepository`.

## Safety

`OutputGuard` performs deterministic last-mile checks for financially sensitive output. Price statements can be checked against approved quote values, and approval assertions can be blocked unless the surrounding workflow has a recorded human/customer checkpoint.

## Deliberately not included

- No LangGraph/CrewAI/Temporal.
- No vector database.
- No dynamic ML model router.
- No vision or voice pipeline.
- No autonomous billing/repair approval.

Those remain outside the Phase 4 scope defined by the blueprint.
