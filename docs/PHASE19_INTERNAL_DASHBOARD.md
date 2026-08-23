# Phase 19 — Internal Dashboard

## Split

PHANTA has two dashboards with different audiences.

### Workshop / Reception Dashboard
Operational only:
- today's bookings
- vehicles waiting
- overdue vehicles
- bookings needing customer confirmation
- unanswered customer messages
- simple WhatsApp connection health
- simple billing state

The workshop dashboard does **not** show AI spend, raw integration errors, Meta technical diagnostics, repair approvals, workshop pricing, or CRM analytics.

### PHANTA Platform Admin Dashboard
Platform-operator only:
- Meta/WhatsApp connection health across tenants
- billing/subscription state across tenants
- OpenAI usage, token counts and estimated cost
- integration errors from Meta and Paystack

## Booking confirmation queue

The old `approval queue` is removed. PHANTA does not authorize repairs or spending. The queue is specifically for **booking requests awaiting an explicit customer booking decision**.

## Routes

- Workshop UI: `GET /dashboard`
- Workshop JSON: `GET /dashboard/data`
- PHANTA owner/admin UI: `GET /platform/dashboard`
- PHANTA owner/admin JSON: `GET /platform/dashboard/data`

The application authentication layer must set `g.tenant_id` for workshop requests and `g.is_phanta_admin` (or `g.platform_admin`) for PHANTA owner requests. This module fails closed when those contexts are absent.

## Deliberate scope exclusions

- No Google Calendar.
- No repair authorisation.
- No quote/pricing management.
- No CRM dashboard.
- No AI decision-making for dashboard state.
