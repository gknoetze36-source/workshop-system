# Paystack Setup

## Railway Variables

```txt
DATABASE_URL=${{Postgres.DATABASE_URL}}
SECRET_KEY=<strong-random-secret>
PUBLIC_BASE_URL=https://your-backend-domain.up.railway.app
FRONTEND_ORIGIN=https://your-vercel-domain.vercel.app
PAYSTACK_SECRET_KEY=sk_live_xxxxxxxxxxxxxxxxx
PAYSTACK_WEBHOOK_SECRET=<paystack-webhook-secret>
```

## Webhook

Set Paystack webhook:

```txt
https://your-backend-domain.up.railway.app/webhook/paystack
```

Final checks:

- Railway uses PostgreSQL `DATABASE_URL`.
- No payment provider secrets are committed.
- Paystack live keys are used only in production.
- The webhook returns `{"ok": true}` for valid signed events.
