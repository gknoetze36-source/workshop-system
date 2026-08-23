# PHANTA Phase 10 — AI Integration Layer

## Scope

Phase 10 implements PHANTA's AI integration layer with **OpenAI/ChatGPT as the only active AI provider for now**.

The architecture still uses a vendor-neutral `AIProvider` interface so a second provider can be introduced later without rewriting PHANTA business logic. However, Phase 10 does **not** configure, install, call, or fall back to Anthropic, Gemini, OpenRouter, or any other provider.

### Included

- Provider interface
- OpenAI/ChatGPT provider
- Environment-configurable GPT model selection
- Central dispatcher
- Exponential retry on transient OpenAI failures
- In-process rate limiting
- Token/latency/cost logging
- Deterministic output guard
- Versioned prompt registry
- Provider-neutral conversation/tool loop foundation

### Explicitly not included

- Anthropic provider
- Gemini provider
- OpenRouter provider
- Any secondary/fallback AI provider
- Booking engine implementation
- Service Advisor business workflows beyond the AI loop/tool contract
- Service recommendation rules
- Quote finalization
- Autonomous repair approval
- Multi-agent orchestration
- Vector database

## Provider contract

`integrations/ai/providers/base_provider.py` is the only contract business logic uses. The only concrete provider in Phase 10 is `OpenAIProvider`.

Provider SDK/API details stay inside the provider adapter.

## Routing

`integrations/ai/models/model_config.py` maps PHANTA task types to OpenAI models:

- `conversation`
- `summarization`
- `extraction`

The provider is deliberately fixed to `openai`.

Model names and cost values are environment-configurable. This allows the GPT model to be changed without changing application code.

## Retry policy

Transient failures such as timeouts, connection failures, HTTP 429 and HTTP 5xx are retried with exponential backoff.

After retries are exhausted, Phase 10 **fails the AI operation** rather than silently switching to another provider. The Service Advisor can therefore escalate to the existing human-handoff path.

Malformed/validation errors are not retried by the provider adapter.

## Safety

AI output is checked before becoming a customer message. Prices can be constrained to approved values and approval language can be blocked until an explicit recorded approval exists. Tool calls are allowlisted and validated server-side with tenant ownership checks.

## Required secret

- `OPENAI_API_KEY`

No provider key belongs in source control.

## Model configuration

Recommended environment variables:

- `OPENAI_API_KEY`
- `PHANTA_AI_MODEL` — shared default model
- `PHANTA_AI_CONVERSATION_MODEL` — optional conversation override
- `PHANTA_AI_SUMMARY_MODEL` — optional summary override
- `PHANTA_AI_EXTRACTION_MODEL` — optional extraction override
- `PHANTA_AI_CONVERSATION_INPUT_COST_PER_M`
- `PHANTA_AI_CONVERSATION_OUTPUT_COST_PER_M`
- `PHANTA_AI_SUMMARY_INPUT_COST_PER_M`
- `PHANTA_AI_SUMMARY_OUTPUT_COST_PER_M`
- `PHANTA_AI_EXTRACTION_INPUT_COST_PER_M`
- `PHANTA_AI_EXTRACTION_OUTPUT_COST_PER_M`

Do not add `PHANTA_AI_FALLBACK_PROVIDER` or other provider keys for Phase 10.

## Production configuration

Set the OpenAI API key and selected GPT model in Railway secrets/config. Keep model names aligned with the currently supported OpenAI model catalogue.

## Future expansion

A second provider can be added later by implementing the existing `AIProvider` contract and extending the dispatcher/configuration. That is intentionally deferred so Phase 10 remains simple, cheap to operate and easy to debug.

## Validation

Phase 10 must pass the complete PHANTA test suite and the OpenAI adapter/dispatcher tests before moving to Phase 11.
