# PHANTA Build Order — Least Work → Most Work

## Phase 0 — Planning and external setup
**Work:** Very low code / high leverage.
- Confirm v1 scope.
- Decide new/dedicated WhatsApp number only for v1 unless coexistence is explicitly required.
- Create provider accounts.
- Start Meta Business Verification early.
- Create Paystack Test Mode account.
- Create OpenAI API account/key for the primary AI provider.
- Prepare privacy policy, terms, data deletion URL.
- Prepare Railway environments/secrets structure.
- Choose the workshop review-link configuration (Google Review URL or HelloPeter URL). No review API is required.

**Why first:** external verification can take longer than coding.

## Phase 1 — Skeleton + environment
- Create integration folders.
- Add environment variable contract.
- Add logging/config foundations.
- Add migration structure.
- Add test structure.
- No external calls yet.

## Phase 2 — Database foundation
Create the relational source of truth:
- customers
- vehicles
- bookings
- services
- conversations
- messages
- recommendations
- quotes
- quote_line_items
- approvals
- follow_ups
- tasks
- audit_logs
- conversation_summaries
- tool_executions
- integration-specific connection/webhook/audit tables

**Milestone:** application can store and retrieve all core PHANTA state without AI.

## Phase 3 — Paystack one-off payments
Fastest complete external integration.
- Test account/keys.
- initialize transaction.
- callback UX.
- verify transaction.
- webhook signature verification.
- idempotency.
- refund.
- reconciliation placeholder.

**External work:** business verification and settlement setup can wait for live mode.

## Phase 4 — Meta authentication foundations
- Meta Developer Account.
- Business Portfolio.
- Meta App.
- WhatsApp product.
- Facebook Login for Business configuration.
- App domains.
- HTTPS.
- App roles.
- System User.
- required permissions.

**External work:** Business Verification should already be underway.

## Phase 5 — Meta Embedded Signup
- Frontend Connect WhatsApp button.
- JS SDK popup.
- v4 configuration.
- callback/session handling.
- backend callback endpoint.
- code exchange.
- tenant connection persistence.

**Milestone:** a workshop can connect a WABA and PHANTA receives usable connection data.

## Phase 6 — Meta token management
- Encrypt tokens.
- debug_token.
- expiry/status checks.
- reconnect state.
- scheduled expiry monitor.
- dashboard connection health.

## Phase 7 — Meta phone registration
- register number.
- PIN handling.
- request/verify code where applicable.
- WABA/phone verification status.

**Status:** COMPLETE

## Phase 8 — Meta webhooks
- GET handshake.
- raw-body signature verification.
- event dedupe.
- inbound messages.
- delivery statuses.
- account update.
- template status.
- quality updates.

**Milestone:** Meta messaging plumbing is proven before AI.

## Phase 9 — Meta messaging + templates
- outbound message client.
- message persistence.
- retry policy.
- utility templates.
- template status tracking.
- customer 24-hour-window handling.

**Recommended test:** human-operated echo bot before AI.

## Phase 10 — AI integration layer — COMPLETE
- provider interface.
- OpenAI provider as the v1 provider.
- model config.
- dispatcher.
- usage/cost logging.
- output guard.
- prompt registry.
- retries/rate limiting.
- Keep the provider abstraction so a second provider can be added later without rewriting business logic.

**Milestone:** PHANTA can call an AI provider without business logic depending on a vendor SDK.

## Phase 11 — Booking engine
- availability queries.
- conflict prevention.
- booking creation.
- status changes.
- confirmations.
- reminder scheduling.
- calendar remains a sync target, not source of truth.

**Phase 11 status: COMPLETE.**

## Phase 12 — Service Advisor v1 — COMPLETE
- customer detection.
- vehicle discovery.
- context builder.
- conversation manager.
- tool definitions.
- tool dispatcher.
- WhatsApp reply loop.
- structured extraction.

**Milestone:** customer can arrive from WhatsApp → identify themselves → identify vehicle → describe problem → book.

**Status:** COMPLETE. The Service Advisor is wired as a single OpenAI function-calling loop, with deterministic customer identity, tenant-safe vehicle access, Phase 11 booking delegation, structured extraction, output guarding, and post-commit WhatsApp reply processing.

## Phase 13 — Service recommendation rules — COMPLETE
- deterministic service interval rules.
- `get_due_services`.
- recommendation records.
- AI explanation only after rule engine works.

**Phase 13 status:** COMPLETE. The deterministic `ServiceRuleEngine` is the maintenance
source of truth, selects the most specific tenant/vehicle rule, evaluates mileage/time
intervals, persists idempotent open recommendation records, and exposes the result
through the Service Advisor `get_due_services` tool. The Service Advisor prompt
explicitly prevents the AI from inventing maintenance intervals.

## Phase 14 — REMOVED
Quote drafting, repair diagnosis for pricing, labour-price lookup, AI quote creation and repair authorisation are **not PHANTA features**. PHANTA is not a workshop CRM/estimating system and does not determine workshop pricing.

## Phase 15 — Booking confirmation & audit — CUSTOM PHANTA SCOPE
This replaces the original Build Order approval/authorisation phase.
- explicit customer yes/no for **booking only**.
- booking is initially `pending`; only an explicit customer response can confirm or cancel it.
- immutable booking confirmation record.
- preserve the customer's raw confirmation message.
- timestamp and channel.
- audit trail.
- output guard prevents the AI from claiming a booking is confirmed before PHANTA records the confirmation.
- no repair authorisation, price approval, parts approval or spending approval.
- customer-facing booking uses **date + morning arrival only**; no exact time slots are shown.
- customer wording: bring the vehicle when the workshop opens. Exact internal scheduling times remain implementation data only.

## Phase 16 — Lifecycle communication — CUSTOM PHANTA SCOPE
Wire only the PHANTA lifecycle messages we actually want. PHANTA does not authorize repairs, determine prices, or act as a CRM.

- `booking_confirmed` — sent when the customer's explicit YES confirms the booking. Customer sees **date + morning** and is told to bring the vehicle when the workshop opens.
- `booking_reminder` — sent at **18:00 the day before** the booking. No exact appointment time is shown to the customer.
- `ready_for_collection` — staff presses the **Ready for collection** button on the reception dashboard; PHANTA sends the customer a WhatsApp message.
- `work_to_be_done` — reception records whether the outstanding work is finished. If work is still outstanding, PHANTA schedules a reminder for the following month.
- `yearly_message` — deterministic annual service reminder based on the vehicle's latest recorded service.

The following original Build Order lifecycle states are intentionally not used as automatic customer messages in this phase: the internal vehicle/repair progression states.

## Phase 17 — Follow-ups — DETERMINISTIC FOUNDATION COMPLETE
Start deterministic:
- `service_due` — driven by the Phase 13 rule engine; only actually due recommendations create follow-ups.
- `booking_reminder` — 18:00 the day before, date + morning only.
- `ready_for_collection_nudge` — deterministic nudge while the booking remains ready for collection; default 24-hour delay, configurable.

**Phase 17 deterministic status: COMPLETE.**

Still separate future work:
- AI-personalized win-back.
- human review of first batches.
- opt-out handling.

## Phase 18 — Post-service review link
Very small feature; no external review-provider API:
- workshop stores a Google Review URL or HelloPeter URL.
- workshop can enable/disable review requests.
- vehicle/service completion triggers the configured WhatsApp message.
- message contains the plain URL as copyable text.
- no button, OAuth, Google Cloud project, Google API or HelloPeter API.

**Phase 18 status: COMPLETE.**

## Phase 19 — Split internal dashboards
PHANTA has two separate dashboards with different audiences.

### Workshop / Reception Dashboard
Operational information only:
- today's bookings.
- vehicles waiting.
- overdue vehicles.
- **bookings needing confirmation** (replaces the old approval queue; this is booking confirmation only).
- unanswered customer messages.
- simple WhatsApp connection health.
- simple billing state.

The workshop dashboard does **not** expose AI spend/cost, raw integration diagnostics, repair authorisation, workshop pricing, quotes, or CRM analytics.

### PHANTA Owner / Platform Admin Dashboard
Platform-operation information only:
- connection health across workshop integrations.
- billing/subscription state across tenants.
- AI usage and estimated cost.
- integration errors (Meta/Paystack and other platform-level failures).

### Scope rules
- `approval queue` is removed from Phase 19. PHANTA does not authorize repairs or spending.
- Booking confirmation remains the only Yes/No decision PHANTA records.
- Workshop users see operational information; PHANTA operators see platform/integration information.

**Phase 19 status:** IMPLEMENTED.

## Phase 20 — Production readiness — IN PROGRESS

### Current clean-master audit
The consolidated application has been structurally audited for syntax, templates, routes, duplicate functions, imports, loops, startup paths, frontend/backend wiring, Meta configuration and deployment files.

Current repair work includes:
- canonical `database/` package; the duplicate top-level `database.py` module is removed.
- SQLAlchemy session helpers are exported from the canonical database package.
- automatic demo-account creation is disabled; the nine industry templates remain seeded.
- legacy Branch service compatibility is removed in favour of canonical Location functions.
- Phase 14 quote-drafting skeleton files and the unreferenced legacy MCP pricing/time-slot server are removed.
- legacy Meta messaging encryption is consolidated onto `META_TOKEN_ENCRYPTION_KEY`.
- current environment variables are documented in `.env.example`.
- current README/build documentation no longer describes the application as skeleton-only.

### Remaining validation gates
- complete Python import graph must pass.
- full Flask runtime/route smoke test.
- PostgreSQL integration/RLS/EXCLUDE/append-only tests.
- Meta webhook and messaging lifecycle tests.
- Paystack lifecycle/reconciliation tests.
- frontend/backend request-flow tests.
- Docker build and Gunicorn startup.
- Railway staging deployment and health verification.

**Phase 20 is not complete until those runtime gates pass.**

