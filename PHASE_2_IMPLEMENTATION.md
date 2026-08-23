# PHANTA Phase 2 — Database Foundation Implementation

## Goal
Create the relational source of truth before building the external integrations and AI conversation loop.

## Implemented now
1. Tenant/workshop source record.
2. Customers and tenant-scoped WhatsApp lookup.
3. Vehicles and customer ownership.
4. Bookings and scheduling fields.
5. Services/service history.
6. Conversations and messages.
7. Deterministic recommendations.
8. Quotes and quote line items.
9. Append-only approvals.
10. Follow-ups.
11. Internal tasks.
12. Audit logs with before/after JSON.
13. Conversation summaries.
14. AI tool execution records.
15. Meta connection/permission/review/webhook/audit records.
16. Paystack customer/payment/subscription/plan/invoice/refund/webhook records.
17. AI usage and prompt version records.
18. Review platform/URL configuration on the Tenant record.

## Security/data rules already represented
- Tenant ownership is explicit on tenant-owned records.
- Customer WhatsApp numbers are unique per tenant.
- External webhook keys support idempotency.
- Approvals expose only an append operation in the repository layer.
- Integration credentials are represented as secret references, not plaintext secrets.
- Audit logs capture actor, action, entity, before and after.
- Tool executions capture arguments, result, success and latency.

## Still required before Phase 2 is production-complete
- Alembic environment and generated migration against the exact deployed PostgreSQL schema.
- PostgreSQL Row-Level Security policies.
- PostgreSQL EXCLUDE constraint for active booking overlap.
- Full repository test suite.
- Transaction/concurrency tests.
- Secret scanning.
- Production backup/recovery verification.

## Deliberate v1 decisions
- Google Calendar is not part of PHANTA v1.
- Google Review and HelloPeter are simple stored URLs, not APIs.
- OpenAI is the planned primary AI provider for v1.
- A provider abstraction remains so another AI provider can be added later.
- No vector database is required for the v1 vehicle/conversation memory model.
