# Phase 3 — Paystack external work

## We must do outside the code
1. Paystack account: confirm Test Mode access.
2. Test keys: place `PAYSTACK_SECRET_KEY` and `PAYSTACK_PUBLIC_KEY` in local/Railway secrets; never commit them.
3. HTTPS webhook: configure the PHANTA `/integrations/paystack/webhook` endpoint in Paystack Dashboard.
4. Callback URL: configure the PHANTA payment result page; remember it is only a customer UX signal.
5. Test transactions: run successful, declined and insufficient-funds test cases.
6. Webhook test: confirm the exact raw-body signature reaches PHANTA and duplicate deliveries do not duplicate fulfillment.
7. Refund test: complete a successful test payment and refund it.
8. Subscription test: create a test plan/subscription and exercise renewal failure/cancellation events.
9. Reconciliation: schedule the verification backstop before production.
10. Production later: complete business verification, settlement bank configuration, live keys, live webhook, dunning and infrastructure IP controls.

## Not required now
- Live keys
- Real customer payments
- Business verification for starting Test Mode
- Google Calendar
