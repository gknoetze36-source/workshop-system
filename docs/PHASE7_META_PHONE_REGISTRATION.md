# PHANTA Phase 7 — Meta Phone Registration

## Scope

Phase 7 implements the Build Order items:

- register the workshop phone number for Cloud API
- handle the 6-digit two-step verification PIN safely
- request a phone verification code by SMS or voice where applicable
- verify the received code
- retrieve WABA and phone verification/display information

Phase 8 webhooks and Phase 9 outbound messaging remain outside this phase.

## Customer-scoped token

All phone/WABA operations use the workshop's Embedded Signup business token stored by Phase 6. The token is decrypted only for the outbound Graph API request.

The PHANTA System User token is not used as a substitute for the workshop's customer-scoped token.

## Endpoints

- `POST /integrations/meta/phone/register`
  - body: `{ "pin": "123456" }`
  - calls `/{phone_number_id}/register`
- `POST /integrations/meta/phone/request-code`
  - body: `{ "code_method": "SMS", "language": "en_US" }`
  - calls `/{phone_number_id}/request_code`
- `POST /integrations/meta/phone/verify-code`
  - body: `{ "code": "123456" }`
  - calls `/{phone_number_id}/verify_code`
- `POST /integrations/meta/phone/pin`
  - body: `{ "pin": "123456" }`
  - calls `/{phone_number_id}` with the new PIN
- `GET /integrations/meta/phone/info`
  - reads `display_phone_number`, `verified_name`, and `quality_rating`
- `GET /integrations/meta/phone/waba`
  - reads WABA name, timezone and message-template namespace

## PIN/code security

PINs and verification codes are request-only secrets. PHANTA validates them, sends them to Meta, then discards them. They are not stored in the database, returned by API responses, or logged.

Meta's documented flow has no endpoint to disable two-step verification once set; the Phase 7 API therefore exposes `set_pin` rather than a disable operation.

## Status

Phone verification status is stored in the existing `meta_business_verification_status.phone_verification_status` field. Phase 7 uses these application states:

- `registered`
- `code_requested`
- `verified`

Phone display information and quality rating are persisted on `meta_business_connections`.
