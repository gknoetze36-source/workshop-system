# PHANTA Frontend Truthfulness Pass — 2026-08-11

## Scope
Frontend-only corrections requested after the truthfulness audit. Backend Python/service/query files were not modified.

## Fixed
1. Removed the unavailable Client Audit Ghost suggestion.
2. Removed Customer fallback labels from the rebuilt workshop dashboard.
3. Removed Platform fallback labels from the SuperAdmin integration-error table.
4. Removed Not supplied fallback timestamps from the SuperAdmin integration-error table.
5. Hidden workshop billing when the backend reports `not_configured` rather than displaying that as a subscription state.
6. Made PHANTA Ghost explicitly data-aware: it only describes facts supplied through `PHANTA_GHOST_DATA` and does not invent counts, statuses, clients, health, currencies, or records.
7. Service Advisor remains absent from navigation because no verified UI route was supplied by the backend.
8. Workshop dashboard search is explicitly limited to today's loaded bookings; it does not claim to be a global customer/vehicle search.
9. Re-checked the rebuilt workshop/platform shell for the specified fallback phrases and removed the relevant display fallbacks.

## Backend intentionally untouched
- Ready-for-collection duplicate-send edge case.
- No global customer/vehicle search endpoint.
- No per-client SuperAdmin audit read model.
- No backend PHANTA Ghost/assistant endpoint.

## Validation
- Python syntax: PASS
- Jinja parsing for base/workshop/platform templates: PASS
- Ghost JavaScript syntax: PASS
- No requested fallback phrases remain in the rebuilt dashboard shell.
