# Paystack Runbook

## Phase 3 code status: COMPLETE

The application-side Paystack integration is complete against the approved architecture. It includes transactions, verification, refunds, customers, plans, subscriptions, overage charging, webhook security/idempotency, event handlers, dunning and reconciliation.

## Test-mode activation

1. Create/confirm the Paystack account in Test Mode.
2. Put `PAYSTACK_SECRET_KEY` and `PAYSTACK_PUBLIC_KEY` in local/Railway secrets.
3. Configure the HTTPS endpoint `/integrations/paystack/webhook` in Paystack Dashboard.
4. Use the callback only to display payment status; never mark a payment paid from the callback.
5. Run successful, declined and insufficient-funds test cases.
6. Confirm duplicate webhook deliveries do not duplicate state changes.
7. Run a refund and confirm `refund.processed` is persisted.
8. Create a test subscription and exercise create, payment failure and disable events.
9. Schedule `run_paystack_reconciliation()` as the missed-webhook backstop.

## Production gate

Do not switch to live keys until the complete initialize → hosted checkout → webhook → verify → fulfillment → refund flow has passed, including invalid-signature and duplicate-webhook tests. The Paystack blueprint also requires dunning and reconciliation before go-live.
