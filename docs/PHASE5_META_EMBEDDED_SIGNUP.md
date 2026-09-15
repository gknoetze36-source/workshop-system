# PHANTA Phase 5 — Meta Embedded Signup

> **Superseded in part.** This document describes the Meta App
> architecture as it stood when VANTA used a single shared Meta App.
> VANTA now runs two independent Meta Apps (WhatsApp and Flyer Lady).
> See [`META_APP_ARCHITECTURE.md`](META_APP_ARCHITECTURE.md) for the
> current architecture. The phase record below is retained as accurate
> history of its own phase.


Implements the official BUILD_ORDER scope: Connect WhatsApp UI, Meta JS SDK v4 launch, callback/session handling, backend code exchange, and tenant connection persistence. Phase 6 owns token encryption/expiry; Phase 7 phone registration; Phase 8 webhooks.

The browser receives only public App ID/config ID/version. The App Secret remains server-side. The authorization code is single-use and is never stored. A short-lived server session nonce prevents callback replay. The raw exchanged customer token is deliberately not stored until Phase 6's encrypted token store is implemented.

External setup after coding: create the Facebook Login for Business configuration, obtain `config_id`, configure Allowed Domains/launch domain with HTTPS, and supply `META_APP_ID`, `META_APP_SECRET`, `META_EMBEDDED_SIGNUP_CONFIG_ID` through secrets.
