# PHANTA Phase 8 — Meta Webhooks

## Scope

Phase 8 establishes the inbound Meta/WhatsApp webhook plumbing before AI or outbound messaging logic.

## Implemented

- GET webhook verification handshake.
- Exact raw `hub.challenge` response.
- POST `X-Hub-Signature-256` verification against untouched request bytes.
- Constant-time signature comparison.
- Durable webhook event storage.
- Database-backed event idempotency.
- Tenant resolution from `phone_number_id` or WABA ID.
- Inbound WhatsApp message persistence when the customer already exists.
- Delivery status updates for persisted WhatsApp messages.
- `account_update` handling.
- `message_template_status_update` audit capture.
- `phone_number_quality_update` connection updates.
- Durable capture of unsupported/auxiliary webhook fields through the webhook event table.

## Routes

`GET /webhooks/meta`

`POST /webhooks/meta`

## Required environment variable

`META_WEBHOOK_VERIFY_TOKEN`

The Meta App Secret is already supplied through `META_APP_SECRET` and is used for POST signature verification.

## Important Phase boundary

Phase 8 does not implement outbound WhatsApp sending, template CRUD, retry queues, 24-hour customer-window decisions, or AI replies. Those remain Phase 9+ responsibilities.

## Security

Customer webhook content is untrusted input. Signature verification occurs before JSON dispatch. Secrets are never stored in webhook event payloads by this implementation.
