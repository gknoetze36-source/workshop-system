# Billing Playbook

Billing code is implemented in:

- `platform_helpers.py`
- `services/paystack.py`
- `app.py`
- `templates/manage_franchises.html`
- `database.py`

## Provider

Payment provider:

- Paystack

Service functions:

- `services.paystack.initialize_transaction()`
- `services.paystack.verify_transaction()`
- `services.paystack.valid_webhook_signature()`
- `services.paystack.claim_webhook_event()`
- `services.paystack.mark_webhook_event_processed()`

## Billing Tables

- `chatbot_usage_daily`
- `chatbot_usage_monthly`
- `usage_daily`
- `billing_records`
- `paystack_webhook_events`
- `franchises`

## Customer Lifecycle

1. Franchise is created in `/manage/franchises`.
2. Plan and billing values are stored on `franchises`.
3. Message usage accumulates through `track_message_usage()`.
4. Monthly usage is closed with `close_billing_period()`.
5. Payment link is generated through Paystack.
6. Paystack webhook activates subscription.

## Usage Tracking

Function:

- `platform_helpers.track_message_usage(franchise_id, count=1)`

Updates:

- `usage_daily`
- `chatbot_usage_monthly`
- `franchises.messages_used`

Called from:

- `send_cheapest_message()`
- `auto_send_reminder()`

## Close Billing Period

Route:

- `POST /billing/close-month`

Function:

- `app.close_billing_month()`

Helper:

- `platform_helpers.close_billing_period()`

Output:

- updates `chatbot_usage_monthly`
- creates/updates `billing_records`
- writes audit action `billing_period_closed`

## Payment Link Flow

Route:

- `POST /billing/<int:billing_id>/payment-link`

Function:

- `app.generate_payment_link()`

Helper:

- `platform_helpers.create_payment_link()`

Paystack call:

- `services.paystack.initialize_transaction()`

Reference:

```text
billing-{billing_records.id}-{billing_period}
```

Metadata:

- `franchise_id`
- `billing_period`
- `billing_record_id`

Audit:

- `billing_payment_link_generated`

## Webhook Flow

Route:

- `POST /webhook/paystack`

Function:

- `app.paystack_webhook()`

Flow:

1. Read raw body.
2. Validate signature with `valid_webhook_signature()`.
3. Parse event, reference, metadata.
4. Claim event with `claim_webhook_event()`.
5. If duplicate, return `{"ok": true, "duplicate": true}`.
6. If event is `charge.success`, call `verify_transaction()`.
7. If Paystack confirms success, call `mark_billing_paid()`.
8. Mark webhook event processed.
9. Write `paystack_payment_success` audit log.

## Subscription Activation

Function:

- `platform_helpers.mark_billing_paid(franchise_id, billing_period, payment_reference="")`

Effects:

- sets `franchises.subscription_status='active'`
- sets `subscription_start=today`
- sets `subscription_end=30 days from now`
- resets `franchises.messages_used=0`
- marks `chatbot_usage_monthly.payment_status='Paid'`
- marks `billing_records.status='paid'`

## Manual Payment Update

Route:

- `POST /billing/<int:billing_id>/payment`

Function:

- `app.update_billing_payment()`

If status is `Paid`, it calls `mark_billing_paid()`.

Audit:

- `billing_payment_updated`

## Renewal Lifecycle

1. Scheduler/cron should run `subscription_check_jobs()` through `cron_jobs.py subscriptions` or scheduled equivalent.
2. `expire_due_subscriptions()` sets expired active franchises to inactive.
3. Billing close creates next period amount.
4. Payment webhook reactivates subscription.

## Failures

Payment link generation fails if:

- `PAYSTACK_SECRET_KEY` is missing.
- Paystack API is unavailable.
- `BILLING_EMAIL`/`ADMIN_EMAIL` fallback email is invalid for production use.

Webhook fails if:

- `PAYSTACK_WEBHOOK_SECRET` is wrong.
- body signature mismatches.
- Paystack verification API is unavailable.

Duplicate webhook:

- safely ignored through `paystack_webhook_events.event_id`.

## Refunds and Cancellations

No refund implementation exists in repository.

No Paystack cancellation API integration exists.

Manual cancellation:

1. Open `/manage/franchises`.
2. Set `subscription_status` to `cancelled` or `inactive`.
3. Set `active=false` if client should lose access.
4. Keep billing records for audit.

## Operational Checks

Daily:

- Check failed Paystack webhook events in `/admin/client-audit`.
- Check `billing_records` with unpaid status.

Monthly:

- Run close month.
- Generate payment links.
- Reconcile Paystack dashboard with `billing_records`.

Quarterly:

- Review overage pricing.
- Confirm `PAYSTACK_WEBHOOK_SECRET` has not drifted.

## Risks

- No refunds/cancellations automation.
- `billing@example.com` fallback should not be used in production.
- Subscription end is always 30 days from payment, not calendar monthly.
- `paystack_webhook_events` has no retention policy.
