# PHANTA Migration Step 8 — AI / OpenAI Ownership

## Active architecture
PHANTA → Owner → Location → AI capabilities (Ghost, Service Advisor, other approved AI functionality).

## Separation
- WhatsApp/communication remains the messaging/integration transport. Service Advisor may use the approved messaging tool but does not own Meta credentials or transport code.
- Service Advisor remains the vehicle/service AI workflow.
- Flyer Lady remains a separate public-promotion/publishing subsystem and is not a Service Advisor tool.
- Ghost remains a separate dashboard/location data assistant and is not merged into the Service Advisor runtime.

## OpenAI
Canonical provider: `integrations.ai.providers.openai_provider.OpenAIProvider`.
Required production secret: `OPENAI_API_KEY`.
Optional configuration: `OPENAI_BASE_URL`, `PHANTA_AI_MODEL`, `PHANTA_AI_CONVERSATION_MODEL`, `PHANTA_AI_SUMMARY_MODEL`, `PHANTA_AI_EXTRACTION_MODEL`, and corresponding cost environment variables.
No API key is stored or invented by this migration.

## Ownership and leakage controls
Service Advisor context verifies the active Location and its Owner before building AI context. Customer, vehicle, booking, conversation and summary records remain location-scoped. Model-supplied IDs never establish ownership. Tool execution uses server-side `ToolContext.location_id` and is audited. The AI prompt explicitly prohibits cross-owner/location access. The selected Location `industry` is included in trusted context.

## Readiness
**Code ready:** provider, dispatcher, retries, rate limiting, usage logging, tools, location-scoped context and separation boundaries.
**Credential required:** real `OPENAI_API_KEY` in Railway/application secrets.
**Production configuration required:** select a currently supported OpenAI model, configure model/cost settings as needed, and test account/project permissions and spending limits.

## Privacy note
OpenAI receives the conversation and the explicitly constructed PHANTA context needed for the Service Advisor task. Production privacy/data-retention settings must still be verified against the organization's requirements and OpenAI account configuration.
