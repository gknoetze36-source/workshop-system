# Meta WhatsApp Cloud API And Paystack Setup

## Required Railway Variables

```txt
DATABASE_URL=${{Postgres.DATABASE_URL}}
DIRECT_URL=${{Postgres.DATABASE_URL}}
JWT_SECRET=<strong-random-secret>
SESSION_SECRET=<strong-random-secret>
PUBLIC_BASE_URL=https://your-backend-domain.up.railway.app
FRONTEND_ORIGIN=https://your-vercel-domain.vercel.app
META_GRAPH_API_VERSION=v20.0
META_APP_SECRET=<meta-app-secret>
PAYSTACK_SECRET_KEY=sk_live_xxxxxxxxxxxxxxxxx
PAYSTACK_WEBHOOK_SECRET=<paystack-webhook-secret>
```

Store each workshop's provider credentials in `messaging_accounts`:

```txt
workshop_id=<workshops.id>
provider=meta
channel=whatsapp
account_id=<meta-business-account-id>
sender_id=<meta-phone-number-id>
access_token=<meta-permanent-access-token>
webhook_verify_token=<strong-random-token>
```

For Twilio:

```txt
provider=twilio
channel=whatsapp or sms
account_id=<twilio-account-sid>
sender_id=<twilio-sender-number>
auth_secret=<twilio-auth-token>
```

Mirror Meta WhatsApp in `whatsapp_numbers`:

```txt
workshop_id=<workshops.id>
phone_number=+27xxxxxxxxx
whatsapp_phone_number_id=<meta-phone-number-id>
meta_business_account_id=<meta-business-account-id>
access_token=<meta-permanent-access-token>
webhook_verify_token=<strong-random-token>
```

## Meta Webhook

Configure Meta App dashboard:

```txt
Callback URL: https://your-backend-domain.up.railway.app/webhooks/meta/whatsapp
Verify token: workshop webhook_verify_token
Fields: messages
```

Inbound webhook handling:

1. Verify `X-Hub-Signature-256`.
2. Resolve workshop by `metadata.phone_number_id`.
3. Insert inbound row in `whatsapp_messages`.
4. Queue automation or AI response work.

## Paystack

Set Paystack webhook:

```txt
https://your-backend-domain.up.railway.app/webhook/paystack
```

Final checks:

- Railway uses PostgreSQL `DATABASE_URL`.
- No messaging provider secrets are committed.
- Each workshop has one active `whatsapp_numbers` row.
- Meta webhook sends and receives test messages.
- Paystack live keys are used only in production.
