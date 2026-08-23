# PHANTA Step 6 — WhatsApp / Meta Ownership

## Canonical ownership

```text
OWNER
  └── LOCATION
       └── MetaBusinessConnection
            ├── business_id
            ├── waba_id
            ├── phone_number_id
            └── encrypted customer access token
```

There is no franchise-level or branch-level WhatsApp ownership in the active integration path.

## Location routing

1. An Owner/Location starts Embedded Signup using its authenticated `location_id`.
2. Meta returns the customer's business/WABA/phone identifiers and access token.
3. PHANTA stores those identifiers on the Location's `MetaBusinessConnection`.
4. The customer access token is encrypted using `META_TOKEN_ENCRYPTION_KEY`.
5. Inbound Meta webhooks are mapped by `phone_number_id` (and WABA as fallback) to exactly one Location.
6. An unresolved or ambiguous webhook is not processed into location-owned data.
7. Outbound messaging receives `location_id`, loads that Location's connection, decrypts only its token, and sends using its `phone_number_id`.

## Production configuration required

These are **application-level Meta credentials/configuration**, not customer Location credentials:

- `META_APP_ID` — PHANTA Meta application ID.
- `META_APP_SECRET` — PHANTA Meta application secret; used for webhook signature verification and Meta application operations.
- `META_SYSTEM_USER_TOKEN` — PHANTA system-user token required by the existing Meta configuration/health layer.
- `META_WHATSAPP_CONFIG_ID` (or `META_EMBEDDED_SIGNUP_CONFIG_ID`) — numeric Embedded Signup configuration ID.
- `META_GRAPH_API_VERSION` — e.g. the configured `vXX.X` Graph API version.
- `META_APP_DOMAINS` — HTTPS PHANTA application domain(s).
- `META_WEBHOOK_VERIFY_TOKEN` — secret used during Meta webhook verification handshake.
- `META_TOKEN_ENCRYPTION_KEY` — Fernet key used to encrypt Location customer access tokens in the database.

### Location-specific Meta data

These must **not** be placed in source code or as one global customer credential:

- Meta Business ID (`business_id`)
- WhatsApp Business Account ID (`waba_id`)
- WhatsApp Phone Number ID (`phone_number_id`)
- Meta customer/business access token (stored encrypted)

The existing Embedded Signup flow is responsible for obtaining and storing these against the authenticated Location.

## Security invariant

```text
Location A -> phone A -> WABA A -> encrypted token A
Location B -> phone B -> WABA B -> encrypted token B

A cannot send through B's phone/token.
B cannot send through A's phone/token.
```
