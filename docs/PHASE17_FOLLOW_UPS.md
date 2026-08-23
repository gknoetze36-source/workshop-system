# Phase 17 — Deterministic Follow-ups

PHANTA follow-ups start deterministic. The AI is not allowed to decide when a follow-up is due.

## Implemented in this phase

### `service_due`
- Uses the Phase 13 `ServiceRuleEngine` as the source of truth.
- Background processing refreshes open recommendations from deterministic rules.
- Only recommendations that are actually due by mileage or date create a `service_due` follow-up.
- One follow-up is created per recommendation and customer/vehicle identity.
- Message is sent through the existing Meta messaging layer; no AI call is required.

### `booking_reminder`
- Preserves the PHANTA rule from Phase 16: 18:00 on the calendar day before the booking.
- Customer-facing text remains date + morning only.
- Phase 17 owns deterministic processing of this follow-up so it cannot be sent twice by two workers.
- Existing Phase 16 follow-up records are recognized to avoid duplication.

### `ready_for_collection_nudge`
- Phase 16 still sends the immediate ready-for-collection message when reception presses the dashboard action.
- Phase 17 schedules a deterministic nudge if the booking remains `ready_for_collection`.
- Initial default delay is **24 hours**, configurable with `PHANTA_READY_COLLECTION_NUDGE_HOURS`.
- If the vehicle is no longer in `ready_for_collection` when the nudge becomes due, the follow-up is cancelled instead of sent.

## Safety boundaries

- No AI-generated timing decisions.
- No pricing, repair authorization, diagnosis, or CRM behaviour.
- Outbound delivery uses the existing Meta messaging service and its 24-hour-window/template policy.
- Tenant scoping is enforced on discovery, scheduling and delivery.
- Follow-ups are idempotent against existing Phase 16 booking reminders.

## Not implemented yet

The later Build Order items — AI-personalized win-back, human review of first batches, and opt-out handling — remain separate work and are not mixed into this deterministic foundation.
