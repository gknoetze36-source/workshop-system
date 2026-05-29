# Meta WhatsApp Cloud API

## Railway Variables

```txt
DATABASE_URL=${{Postgres.DATABASE_URL}}
DIRECT_URL=${{Postgres.DATABASE_URL}}
JWT_SECRET=<strong-random-secret>
SESSION_SECRET=<strong-random-secret>
PUBLIC_BASE_URL=https://api.vanta.example
FRONTEND_ORIGIN=https://app.vanta.example
META_GRAPH_API_VERSION=v20.0
META_APP_SECRET=<meta-app-secret>
PAYSTACK_SECRET_KEY=<optional>
PAYSTACK_WEBHOOK_SECRET=<optional>
OPENAI_API_KEY=<optional>
```

Per-workshop provider credentials belong in `messaging_accounts`. Meta WhatsApp numbers are also mirrored in `whatsapp_numbers` for webhook lookup:

```txt
workshop_id
provider: meta | twilio
channel: whatsapp | sms
account_id
sender_id
access_token
auth_secret
webhook_verify_token
```

## Integration Structure

```txt
src/
  modules/
    whatsapp/
      providers.ts
      meta-client.ts
      twilio-client.ts
      webhook.controller.ts
      webhook.service.ts
      message.service.ts
      signature.ts
    auth/
      auth.controller.ts
      auth.service.ts
      rbac.ts
    tenancy/
      workshop-context.ts
      tenant-prisma.ts
```

## Webhooks

```txt
GET  /webhooks/meta/whatsapp
POST /webhooks/meta/whatsapp
POST /api/workshops/:workshop_id/whatsapp/send
POST /webhooks/twilio/:channel/:franchise_slug/:branch_slug/:token
```

Verification:

1. Match `hub.verify_token` to an active `whatsapp_numbers.webhook_verify_token`.
2. Return `hub.challenge`.

Inbound POST:

1. Verify `X-Hub-Signature-256` with `META_APP_SECRET`.
2. Resolve tenant by `metadata.phone_number_id`.
3. Load `whatsapp_numbers` by `whatsapp_phone_number_id`.
4. Upsert customer by `(workshop_id, phone_number)`.
5. Insert `whatsapp_messages` with resolved `workshop_id`.
6. Queue AI or automation work asynchronously.

Twilio inbound:

1. Use `/webhooks/twilio/whatsapp/...` or `/webhooks/twilio/sms/...`.
2. Resolve the workshop by `franchises.workshop_id`.
3. Verify `X-Twilio-Signature` with the Twilio `messaging_accounts.auth_secret`.
4. Process the inbound message through the same assistant flow.

Outbound:

1. Require authenticated `workshop_id`.
2. Load active `messaging_accounts` for that workshop and channel.
3. Prefer Meta for WhatsApp; use Twilio for SMS and as WhatsApp fallback.
4. Store provider message ID and status.

## Auth

- Use email/password with Argon2 or bcrypt.
- JWT/session claims: `user_id`, `workshop_id`, `role`.
- Every Prisma query must include `workshop_id`.
- Use RBAC roles: `owner`, `admin`, `advisor`, `technician`, `reception`.
- Write sensitive actions to `audit_logs`.

## Tenant Isolation

- Never trust client-supplied `workshop_id`; derive it from auth.
- Keep composite foreign keys `(workshop_id, id)` for tenant-owned relations.
- Wrap Prisma in a tenant client/helper that injects `workshop_id`.
- Background jobs must carry `workshop_id` in the payload.
- Prisma core tables live in the `vanta_core` PostgreSQL schema.
- The current Flask runtime keeps legacy public tables and maps `franchises.workshop_id` to canonical `workshops.id` during database initialization.

## Scaling To 1,000+ Workshops

- One PostgreSQL database, shared tables, `workshop_id` isolation.
- Use PgBouncer-compatible Prisma connection settings on Railway.
- Move WhatsApp sends, AI work, and reminders to queues.
- Keep `whatsapp_messages` append-only; archive old rows later by date.
- Add read replicas only after query/load metrics justify it.
- Cache workshop config by `whatsapp_phone_number_id`.
- Add rate limits per `workshop_id` and per WhatsApp number.
