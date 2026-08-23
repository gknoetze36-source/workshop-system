# Phase 15 — Booking Confirmation & Audit

## Scope
PHANTA records explicit customer confirmation for a booking. It does not authorize repairs, prices, parts, labour or spending.

## Customer flow
1. PHANTA offers a date.
2. PHANTA describes arrival as **morning, when the workshop opens**.
3. No exact time is shown or requested from the customer.
4. A booking request remains `pending`.
5. Customer replies with an unambiguous yes/no.
6. `YES` changes the booking to `confirmed`; `NO` changes it to `cancelled`.

## Evidence
`booking_confirmations` stores the decision, raw customer message, channel, timestamp, tenant, customer and booking. Each booking can have one immutable decision.

## Safety
- Ambiguous language is rejected.
- The confirmation tool must receive the exact current inbound customer message.
- The AI cannot assert that a booking is confirmed until the confirmation record exists.
- Quote/repair-authorization tools are not exposed to the Service Advisor.

## Internal scheduling
Phase 11 may retain exact `start_time`/`end_time` internally for conflict prevention. Those values are not customer-facing. Phase 15 currently uses an 08:00 internal workshop-opening default until workshop-specific opening configuration is added.
