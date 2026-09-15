# VANTA Meta App Architecture

**Status:** authoritative. Supersedes the Meta App descriptions in
`PHASE4_META_AUTH_FOUNDATIONS.md`, `PHASE5_META_EMBEDDED_SIGNUP.md`,
`PHASE8_META_WEBHOOKS.md` and `FLYER_LADY_IMPLEMENTATION.md`, all of which
were written when VANTA used a single shared Meta App. Those documents are
retained as an accurate record of their own phases; where they conflict
with this file, this file is correct.

---

## The two Meta Apps

VANTA runs **two completely independent Meta Applications**.

```
VANTA
│
├── WhatsApp capability
│   └── Meta App A: "VANTA Automations — WhatsApp"   (the EXISTING app)
│       ├── WhatsApp Embedded Signup
│       ├── WhatsApp Business onboarding / WABA operations
│       ├── Phone number onboarding
│       ├── Customer WhatsApp tokens
│       ├── Messaging and templates
│       ├── Webhooks (signature + verify token)
│       ├── System User and Tech Provider operations
│       ├── Credit-line operations
│       └── WhatsApp App Review
│
└── Flyer Lady capability
    └── Meta App B: "VANTA Automations — Flyer Lady"  (a NEW app)
        ├── Facebook Login for Business
        ├── Facebook Page connection and selection
        ├── Facebook Page access tokens
        ├── Facebook Feed publishing
        ├── Facebook Page Story publishing
        ├── Data deletion + deauthorization callbacks
        └── Flyer Lady App Review
```

---

## What is shared, and what is not

### Shared (these are not Meta App identities)

* The VANTA Business Portfolio
* The VANTA backend, database, tenant/location system, encryption and audit
  infrastructure
* The production domain `app.vantaautomations.co.za` — **a shared domain is
  not a shared Meta App identity**
* `GraphApiClient` (generic HTTP), the signed-request parser, token
  encryption utilities

### Never shared

| | WhatsApp App | Flyer Lady App |
|---|---|---|
| App ID | `META_WHATSAPP_APP_ID` | `META_FLYER_LADY_APP_ID` |
| App Secret | `META_WHATSAPP_APP_SECRET` | `META_FLYER_LADY_APP_SECRET` |
| Graph version | `META_WHATSAPP_GRAPH_API_VERSION` | `META_FLYER_LADY_GRAPH_API_VERSION` |
| App domains | `META_WHATSAPP_APP_DOMAINS` | `META_FLYER_LADY_APP_DOMAINS` |
| Login config | `META_WHATSAPP_CONFIG_ID` | `META_FLYER_LADY_CONFIG_ID` |
| System User | `META_WHATSAPP_SYSTEM_USER_TOKEN` | *(never — see below)* |
| Webhook verify | `META_WHATSAPP_WEBHOOK_VERIFY_TOKEN` | *(never — no webhooks)* |
| OAuth redirect | *(JS SDK, no redirect URI)* | `META_FLYER_LADY_OAUTH_REDIRECT_URI` |

Also never shared: OAuth sessions, OAuth state, token storage, callback
ownership, App Review permissions, failure domains.

---

## Code layout

```
integrations/meta/auth/capability_config.py
├── WhatsAppMetaConfig      -- reads META_WHATSAPP_* (+ legacy fallback)
└── FlyerLadyMetaConfig     -- reads META_FLYER_LADY_* ONLY

integrations/meta/auth/config.py
└── MetaAuthConfig          -- DEPRECATED shim, tests only, no production use
```

### Two properties worth understanding

**1. The legacy fallback is asymmetric, deliberately.**

`WhatsAppMetaConfig` falls back to the old shared `META_APP_ID` /
`META_APP_SECRET`. That is correct: the existing production WhatsApp Meta
App keeps its identity, so those values are its own.

`FlyerLadyMetaConfig` **never reads them**. The Flyer Lady app is new, so no
legacy value could legitimately belong to it. A fallback there would mean
Flyer Lady silently authenticating as the WhatsApp app whenever its own
variables were missing — invisible in code review, invisible at startup,
surfacing only when Meta rejected a live call.

**2. Flyer Lady fails closed on System User operations.**

`FlyerLadyMetaConfig` has no `system_user_token` field at all, and its
`validate_system_user()` always raises. `GraphApiClient.get()` / `.post()`
call that method before using an app-level token, so the forbidden path
fails closed rather than reaching for WhatsApp credentials. Flyer Lady
legitimately uses only `get_with_token()` / `post_with_token()` with a Page
token, which never calls it.

---

## Permissions

### WhatsApp App

```
whatsapp_business_messaging
whatsapp_business_management
business_management
```

Facebook Page permissions must **never** be added to the WhatsApp app
merely because both products belong to VANTA.

### Flyer Lady App

```
pages_show_list        -> MetaSocialGraphClient.list_pages (/me/accounts)
pages_read_engagement  -> reading the connected Page's context
pages_manage_posts     -> publish_feed_photo / publish_photo_story
```

Every permission maps to a real code path, because App Review requires each
one be demonstrated on video and an undemonstrable permission risks
rejection.

**Deliberately excluded:** `pages_manage_metadata`,
`pages_manage_engagement`, `pages_read_user_content`, `read_insights`,
`pages_messaging` (no implementation), and `instagram_basic` /
`instagram_content_publish` (Instagram is postponed until an Instagram
implementation is actually submitted).

**Open decision:** `business_management` is currently excluded from the
Flyer Lady set and must be verified against the real Facebook Login for
Business flow before the App Review submission.

---

## Callbacks

| Purpose | URL | Verified with |
|---|---|---|
| WhatsApp webhook | `/webhooks/meta` | `META_WHATSAPP_APP_SECRET` |
| Flyer Lady OAuth | `/dashboard/flyer-lady/connect/callback` | Flyer Lady app |
| Flyer Lady data deletion | `/integrations/meta/flyer-lady/data-deletion` | `META_FLYER_LADY_APP_SECRET` |
| Flyer Lady deauthorization | `/integrations/meta/flyer-lady/deauthorize` | `META_FLYER_LADY_APP_SECRET` |
| Legacy data deletion | `/data-deletion` | `META_FLYER_LADY_APP_SECRET` |

The legacy `/data-deletion` path is retained because it is already
registered in the Meta App Dashboard; removing it would silently break a
live callback. Both deletion paths resolve to the same Flyer Lady
verification.

**The two authentication flows are deliberately different and must not be
merged to reduce the number of URLs:**

```
WhatsApp:    FB.login()  ->  Embedded Signup  ->  code  ->  backend exchange
Flyer Lady:  OAuth authorization  ->  browser redirect  ->  callback
```

WhatsApp's `/integrations/meta/embedded-signup/callback` is a backend
endpoint reached by the JS SDK, **not** a browser OAuth redirect URI, and
`redirect_uri` must not be added to its token exchange to make the two look
alike.

---

## Deletion and deauthorization boundaries

**Flyer Lady deauthorization** clears the Page token, marks the connection
`revoked`, and deletes OAuth sessions. It does **not** delete the
workshop's specials, posts or click history — that is VANTA business data,
not the user's personal Facebook data, so a user who reconnects finds their
content intact.

It must never touch WABA, WhatsApp phone numbers, System User credentials,
billing, credit-line state or message history. This is structural:
`deauthorize_flyer_lady_user()` only ever queries `MetaSocialConnection` and
`MetaSocialOAuthSession`, and a test asserts that against the function's own
source.

There is **no global "delete all Meta data for this user" operation**, by
design.

### Audit actions

```
meta_flyer_lady_data_deletion
meta_flyer_lady_deauthorization
```

The audit trail identifies which Meta App caused each event. A generic
`meta_data_deletion_callback` could not.

---

## Failure isolation

Both capabilities are independent failure domains:

```
WHATSAPP FAILURE  ≠  FLYER LADY FAILURE
FLYER LADY FAILURE ≠  WHATSAPP FAILURE
```

Enforced and proven:

* Missing, broken or wrong credentials in one app never disable the other
* A Flyer Lady App Secret is rejected by WhatsApp webhook verification
* A WhatsApp App Secret is rejected by Flyer Lady signed-request verification
* `integration_status()` reports each capability against its own variables
  only, and never names the other's
* `scripts/check_env.py` validates each capability independently
* There is **no cross-capability fallback anywhere** — both fail closed with
  a capability-specific error

All of the above are covered by
`tests/security/test_meta_capability_separation.py`.

---

## Migration state

The legacy `META_APP_ID` / `META_APP_SECRET` (and the aliases
`META_GRAPH_API_VERSION`, `META_APP_DOMAINS`, `META_SYSTEM_USER_TOKEN`,
`META_WEBHOOK_VERIFY_TOKEN`, `META_EMBEDDED_SIGNUP_CONFIG_ID`,
`META_SOCIAL_REDIRECT_URI`) still resolve **for the WhatsApp capability
only**, so a production deployment continues to work before the new
variables are populated.

`scripts/check_env.py` warns while any of them remain set. Remove them once
the `META_WHATSAPP_*` variables are populated in Railway.

**No production code references `MetaAuthConfig`.** A test parses every
non-test module and fails on any executable reference, so this cannot
silently regress.
