# PHANTA Phase 4 — Meta Authentication Foundations

This phase follows `BUILD_ORDER.md` exactly.

## In scope
- Meta Developer Account readiness
- PHANTA Business Portfolio linkage/readiness
- Meta App configuration contract
- WhatsApp product readiness
- Facebook Login for Business configuration contract
- HTTPS/app-domain validation
- development app-role readiness
- System User configuration and health check
- canonical required permissions

## Deliberately NOT in Phase 4
- Embedded Signup popup/callback
- authorization-code exchange
- per-workshop token persistence
- token encryption/expiry monitoring
- phone registration
- webhooks
- outbound messaging/templates

Those belong to later Build Order phases.

## Required environment

```text
META_APP_ID=
META_APP_SECRET=
META_GRAPH_API_VERSION=v25.0
META_SYSTEM_USER_TOKEN=
META_APP_DOMAINS=https://your-production-domain.example
```

Never commit the App Secret or System User token.

## External Meta checklist

Before Phase 4 can be externally signed off:

- [ ] Meta Developer Account exists
- [ ] PHANTA Business Portfolio exists and is linked to the app
- [ ] Business Verification is underway/completed as applicable
- [ ] Meta App created as a Business app and linked to PHANTA Business Portfolio
- [ ] WhatsApp product added
- [ ] Facebook Login for Business configuration created
- [ ] Production domain is HTTPS
- [ ] Production domain added to the required Meta allowed-domain/redirect configuration
- [ ] PHANTA engineering users have appropriate Admin/Developer/Test roles
- [ ] PHANTA System User created
- [ ] System User has required business asset access
- [ ] Required permissions are configured:
  - `whatsapp_business_messaging`
  - `whatsapp_business_management`
  - `business_management`
- [ ] System User token stored only in Railway/environment secrets
- [ ] `SystemUserService.health_check()` succeeds against Meta

## Coding completion

The Phase 4 code foundation is complete. All secrets remain blank/placeholders until Meta setup is supplied. The code validates App ID, App Secret, Graph API version, HTTPS domains, System User token presence, required permissions, safe public OAuth configuration, and System User Graph API health checks.

The following values are intentionally supplied later and must never be committed to Git:
- `META_APP_ID`
- `META_APP_SECRET`
- `META_SYSTEM_USER_TOKEN`
- `META_APP_DOMAINS`

`META_EMBEDDED_SIGNUP_CONFIG_ID` is intentionally not required for Phase 4; it belongs to Phase 5.

## Important version decision

The Meta blueprint says Embedded Signup v2 is deprecated on 15 October 2026; PHANTA should build the later Embedded Signup phase against v4. Phase 4 only prepares the authentication foundation; the v4 configuration itself belongs to Phase 5.
