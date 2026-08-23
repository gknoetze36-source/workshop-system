# PHANTA Phase 3 — Paystack Implementation

## Code-complete scope

The Paystack integration now covers the application-side production architecture defined in `PHANTA-Integrations-Blueprint(3).md`:

- Central `PaystackClient` with secret-key authentication and bounded network retry for safe reads.
- Transaction initialize, verify, refund and persisted payment lifecycle.
- Stable transaction references for idempotent initialize/retry behavior.
- Customer creation/linking and Paystack customer-code persistence.
- Plan creation and persistence.
- Managed subscription creation, lookup and cancellation using the Paystack `email_token`.
- Usage/WhatsApp overage charge-authorization seam.
- HMAC-SHA512 verification against the raw webhook body.
- Append-only webhook event persistence and exact-payload idempotency.
- Charge, subscription, invoice, refund and dispute event handling.
- Failed-renewal/dunning state transitions.
- Scheduled reconciliation service for unresolved transactions.
- Secret-reference field for stored payment authorization data; raw authorization tokens must not be placed in the database.
- Unit/integration coverage for the core lifecycle and failure cases.

The database mirrors Paystack records for application logic/audit while Paystack remains the system of record for money movement.

## Required external gate

The code is complete, but the integration is **not production-live** until the following real-account checks pass:

1. Paystack Test Mode account and test keys configured.
2. HTTPS webhook configured in Paystack Dashboard.
3. Successful, declined and insufficient-funds test payments.
4. Real webhook delivery and duplicate delivery test.
5. Verify callback/transaction flow.
6. Refund lifecycle test.
7. Subscription create/cancel/failure test.
8. Reconciliation job exercised against unresolved transactions.
9. Production business verification and settlement configuration.
10. Live keys/webhook and infrastructure controls only after all staging tests pass.

The callback remains UX-only; webhook/verification is authoritative for payment state. Real money is never fulfilled from a browser callback alone.
