# PHANTA Step 7 — Paystack Ownership

## Canonical ownership

```text
PHANTA
└── Owner
    └── Location
        └── Subscription / Billing
            ├── PaymentCustomer
            ├── Payment
            ├── Subscription
            ├── Invoice
            ├── Refund
            └── PaystackWebhookEvent
```

The active Paystack integration uses `location_id` as the business ownership boundary.
There is no active franchise-level or branch-level Paystack ownership.

## Current Paystack architecture

### Checkout / initialization

```text
Location
  ↓
TransactionService / billing_service
  ↓
metadata.phanta_location_id
  ↓
Paystack initialize transaction
  ↓
authorization URL
```

### Customer

A PHANTA payment customer is persisted as:

`PaymentCustomer.location_id -> Location.id`

The Paystack customer metadata includes `phanta_location_id`.

### Payment

`Payment.location_id -> Location.id`

Payment references remain globally unique, while all reconciliation/verification paths can additionally constrain by location.

### Webhook

```text
Paystack webhook
      ↓
signature verification
      ↓
resolve location
      ├── phanta_location_id metadata
      ├── Paystack customer code
      ├── payment reference
      └── subscription code
      ↓
active Location
      ↓
Location-scoped transaction / RLS
      ↓
event handler
```

If a location cannot be resolved, the event is not fulfilled.

### Subscription

`Subscription.location_id -> Location.id`.

Creating a subscription now verifies that the Paystack customer belongs to the requested Location.

### Billing

Monthly billing is calculated from location-scoped usage and creates:

`billing_records.location_id`

Setup fees are stored on the Location as `setup_fee` and can be incorporated by the billing layer when the corresponding billing record is generated. No Paystack key or credential is invented by the application.

Message overage is calculated per Location using the Location's monthly limit and overage price.

### Failed payments

Paystack invoice/payment failures update the Location-owned subscription/payment records through the resolved Location context. Dunning remains application-owned.

### Reconciliation

Unresolved payments can be reconciled globally by the scheduled job, or constrained to a specific `location_id`. Individual verification is also location-scoped when a location is supplied.

## CODE READY

The code contains the application-side pieces for:

- Paystack transaction initialization
- transaction verification
- customer creation/linking
- subscription creation/cancellation
- invoice handling
- payment failure handling
- refunds
- webhook signature validation
- webhook Location resolution
- webhook idempotency
- reconciliation
- dunning
- billing records
- monthly usage/overage calculation
- Location-scoped payment metadata

## CREDENTIALS REQUIRED

Do not place real values in source control.

Required Railway secrets:

- `PAYSTACK_SECRET_KEY`
- `PAYSTACK_PUBLIC_KEY`
- `PAYSTACK_WEBHOOK_SECRET` (the implementation falls back to the secret key if this is absent, but a dedicated webhook secret should be configured where Paystack/account configuration supports it)

The application does **not** contain real production Paystack credentials.

## PRODUCTION CONFIGURATION REQUIRED

Before live use:

1. Paystack business/account setup must be complete.
2. Confirm the Paystack account is enabled for the intended ZAR payment flow.
3. Add the production secret/public keys to Railway.
4. Configure the Paystack webhook URL:
   `/integrations/paystack/webhook`
5. Ensure the public HTTPS Railway URL is used.
6. Confirm Paystack webhook signing is enabled/available and configure the corresponding secret.
7. Run a test checkout.
8. Confirm `charge.success` reaches the webhook.
9. Confirm a failed/abandoned payment is handled.
10. Confirm subscription create/cancel/failure events.
11. Confirm refund handling.
12. Confirm duplicate webhook delivery is idempotent.
13. Confirm reconciliation can recover a missed webhook.
14. Confirm billing records point to the correct Location.
15. Only then replace test keys with live keys.

## Production safety

The Paystack webhook is the payment fulfillment authority. The browser callback is informational and must not mark a bill paid by itself.

Do not run the new Alembic migration against production until the current production database has been backed up and existing `billing_records.location_id` values have been verified. The migration deliberately does not guess ownership for existing NULL values.
