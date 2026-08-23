# Phase 17 Validation

## Scope
Deterministic follow-up foundation only:
- service due
- booking reminders
- ready-for-collection nudges

## Tests
- Full suite: 106 passed, 3 skipped, 2 pre-existing deprecation warnings.
- Python compileall for Phase 17/related packages: PASS.

## Safety
- Follow-up eligibility is deterministic.
- Service due is sourced from the Phase 13 rule engine.
- Booking reminders remain 18:00 the day before.
- Ready-for-collection nudge defaults to 24 hours and is configurable.
- No AI is used to decide timing.
- No repair pricing, authorization, diagnosis, or CRM behaviour is added.
