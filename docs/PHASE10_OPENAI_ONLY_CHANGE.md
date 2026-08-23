# Phase 10 Change — OpenAI/ChatGPT Only

Date: 2026-08-08

## Decision

For the current PHANTA build, **OpenAI/ChatGPT is the only active AI provider**.

Technically, PHANTA integrates with the **OpenAI API**; “ChatGPT” refers to the OpenAI model/product family used through that API.

## What changed

- Removed active Anthropic provider implementation.
- Removed active Gemini provider implementation.
- Removed active OpenRouter provider implementation.
- OpenAI is now the only provider exported by `integrations.ai.providers`.
- Model routing is fixed to `provider="openai"`.
- Removed provider fallback from `ModelRoute`.
- Dispatcher retries transient OpenAI failures but does not switch vendors.
- If OpenAI remains unavailable after retries, the operation fails cleanly and can follow the existing human-handoff path.
- Updated Phase 10 tests and validation.
- Kept the generic `AIProvider` interface so adding another provider later will not require a business-logic rewrite.

## Environment

Required:

```text
OPENAI_API_KEY
```

Optional model configuration:

```text
PHANTA_AI_MODEL
PHANTA_AI_CONVERSATION_MODEL
PHANTA_AI_SUMMARY_MODEL
PHANTA_AI_EXTRACTION_MODEL
```

No Anthropic, Gemini or OpenRouter API keys are required for Phase 10.

## Validation

```text
79 passed
3 skipped
2 warnings
```

Python compilation of `integrations/ai/` also passed.

## Future provider expansion

A second provider should only be added deliberately in a later phase. Until then, PHANTA has one AI vendor path: OpenAI/ChatGPT.
