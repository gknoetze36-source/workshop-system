# VANTA META CONFIGURATION — SOURCE OF TRUTH PACK

**Purpose:** establish everything that must be known, verified and prepared
**before** touching Meta.
**Produced:** 2026-08-27
**Code basis:** `workshop-system-main_22.zip` + completed Meta App Separation build
**Infrastructure basis:** live Railway project `PHANTA Production`
(`8fa04b1b-fe01-4568-9416-35f2b3653efa`), environment `production`

---

## STATUS LEGEND

| Tag | Meaning |
|---|---|
| **ESTABLISHED BY VANTA BUILD** | Verified in the actual codebase. Reliable now. |
| **ESTABLISHED FROM RAILWAY** | Read from the live Railway project. Reliable now. |
| **REQUIRES LIVE META UI VERIFICATION** | Cannot be known until the real Meta screen is open. |
| **REQUIRES CURRENT META DOCUMENTATION VERIFICATION** | Depends on Meta policy/docs that change. |
| **OPERATOR ACTION** | Only the human can do or supply this. |
| **CURRENTLY UNKNOWN** | Not established by any source available here. |

---

## CRITICAL CORRECTIONS TO EARLIER VANTA REPORTS

Three claims made in earlier Step reports were re-checked against the code
while producing this pack and do not hold. Correcting them here, because a
source-of-truth pack that repeats an unverified claim is worse than useless.

### Correction 1 — `pages_read_engagement` has NO direct code path

Earlier reports justified it as "reading the connected Page's context."
**That is not backed by code.** An exhaustive search of every Facebook read
operation in `flyer_lady/` and `integrations/meta/social/` found exactly one:

```
/me/accounts   (list_pages)   -> justified by pages_show_list
```

There is no `GET /{page_id}` anywhere, no insights read, no comment read.

`pages_read_engagement` may still be required as a **Meta-imposed
dependency of `pages_manage_posts`** — that is a documentation question,
not a code question. See Section 7.
**Status: REQUIRES CURRENT META DOCUMENTATION VERIFICATION.**

### Correction 2 — `list_pages` requests an Instagram field

```python
params={"fields": "id,name,access_token,tasks,instagram_business_account", ...}
```

The code requests `instagram_business_account` while VANTA has deliberately
decided **not** to request Instagram permissions. Meta usually omits fields
the token lacks permission for rather than failing the call, but this has
not been proven for this exact request.
**Status: REQUIRES LIVE VERIFICATION once the Flyer Lady app exists.**
Not a blocker for app creation; is a blocker for assuming Page listing
works first try.

### Correction 3 — the `billing` Railway service does NOT need Meta credentials

Earlier reports said to set Meta variables on all three services. Verified
against the code: `railway-cron-billing.toml` runs `run_billing_jobs()` →
`run_billing_cycle` → `services/automatic_billing_service`, which imports
nothing from `integrations/meta`. The `billing` service currently has
`META_APP_DOMAINS` set, which appears to be vestigial.
**Status: ESTABLISHED BY VANTA BUILD.** Meta variables are needed on
`phanta-web` and `phanta-scheduler` only.

---

# SECTION 1 — MASTER REQUIREMENTS MATRIX

Legend for "Known before opening Meta?": **Y** = yes, **N** = no.

## A. Meta account / business requirements

| # | Requirement | WhatsApp App | Flyer Lady App | Current known value | Source | Status | Who | Known before Meta? | If unknown |
|---|---|---|---|---|---|---|---|---|---|
| A1 | Business Portfolio identity | Required | Required (same one) | Name unconfirmed | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | Cannot confirm both apps sit in the same portfolio |
| A2 | Business Verification status | Required before Advanced Access | Required before Advanced Access | Unknown | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | App Review cannot be planned |
| A3 | Portfolio owns the target Facebook Page | N/A | Required | Unknown | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | Page selection may return nothing |
| A4 | Portfolio owns the existing WABA | Required | Must NOT be used | Unknown | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | Risk of touching the wrong WABA |
| A5 | Operator has admin rights on the portfolio | Required | Required | Unknown | — | OPERATOR ACTION | Operator | N | Cannot create or configure apps |

## B. App identity

| # | Requirement | WhatsApp App | Flyer Lady App | Current known value | Source | Status | Who | Known before Meta? | If unknown |
|---|---|---|---|---|---|---|---|---|---|
| B1 | App name | "VANTA Automations — WhatsApp" (target) | "VANTA Automations — Flyer Lady" (target) | Current App A name unknown | Spec | REQUIRES LIVE META UI VERIFICATION | Operator | N | Cosmetic only; App A rename is optional |
| B2 | App ID | Exists | Does not exist yet | Not recorded here | — | REQUIRES LIVE META UI VERIFICATION / CURRENTLY UNKNOWN | Operator | N | Cannot populate Railway |
| B3 | App Secret | Exists | Does not exist yet | Never recorded | — | OPERATOR ACTION | Operator | N | Cannot populate Railway |
| B4 | App type | Unknown | Must support Facebook Login for Business | Unknown | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | May pick wrong creation path |
| B5 | App mode (Dev/Live) | Unknown | Starts in Development | Unknown | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | Affects who can test |
| B6 | Use cases currently attached | Unknown | To be chosen | Unknown | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | Cannot plan Section 3 |

## C. Facebook Login for Business

| # | Requirement | WhatsApp App | Flyer Lady App | Current known value | Source | Status | Who | Known before Meta? | If unknown |
|---|---|---|---|---|---|---|---|---|---|
| C1 | FLB product present | Required (Embedded Signup depends on it) | Required | Unknown | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | Cannot configure login |
| C2 | Login configuration exists | Yes — Embedded Signup variation | Must be created — standard variation | App A config ID set in Railway (name only) | Railway | ESTABLISHED FROM RAILWAY (existence) / value unknown | Operator | Partial | Flyer Lady flow cannot start |
| C3 | Configuration ID | `META_WHATSAPP_CONFIG_ID` exists | Does not exist yet | Value never recorded | Railway | CURRENTLY UNKNOWN (value) | Operator | N | Cannot populate Railway |
| C4 | Exact current FLB settings list | — | — | Historical list only | Spec §4 | REQUIRES LIVE META UI VERIFICATION | Operator | N | Cannot pre-write a settings plan |

## D. WhatsApp

| # | Requirement | WhatsApp App | Flyer Lady App | Current known value | Source | Status | Who | Known before Meta? | If unknown |
|---|---|---|---|---|---|---|---|---|---|
| D1 | WhatsApp product added | Required | Must NOT be present | Unknown | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | Embedded Signup unavailable |
| D2 | WABA exists | Required | N/A | **0 rows in `meta_business_connections`** | Production DB | ESTABLISHED (VANTA side) | — | Y (VANTA side) | — |
| D3 | Phone number registered | Required for live messaging | N/A | Unknown | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | Cannot message |
| D4 | Phone number ID | Required at runtime | N/A | Captured per workshop at signup, not configured | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |
| D5 | Phone PIN registration | Required by Meta's onboarding sequence | N/A | Backend endpoints exist; **no UI calls them** | Code | ESTABLISHED BY VANTA BUILD — **GAP** | Both | Y | Workshop may appear connected but cannot message |
| D6 | `subscribed_apps` call after signup | Required by Meta Tech Provider docs | N/A | **Not implemented anywhere in the codebase** | Code | ESTABLISHED BY VANTA BUILD — **GAP** | Both | Y | No webhooks ever delivered for that customer |
| D7 | Credit line / billing model | `META_CREDIT_SHARING_ENABLED` defaults true | N/A | Variable documented; live state unknown | Code + `.env.example` | REQUIRES LIVE META UI VERIFICATION | Operator | Partial | Billing may route unexpectedly |
| D8 | ZAR unsupported for credit sharing | Affects billing model | N/A | Documented in `.env.example` | Code comment | REQUIRES CURRENT META DOCUMENTATION VERIFICATION | Operator | N | Credit sharing may be impossible in this market |

## E. Flyer Lady

| # | Requirement | WhatsApp App | Flyer Lady App | Current known value | Source | Status | Who | Known before Meta? | If unknown |
|---|---|---|---|---|---|---|---|---|---|
| E1 | Existing connections at risk | N/A | **0 rows in `meta_social_connections`** | Production DB | ESTABLISHED | — | Y | — |
| E2 | Facebook Page to connect | N/A | Required | Unknown | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | Cannot demo Page publishing |
| E3 | Page publishing endpoints | N/A | `/{page_id}/photos`, `/{page_id}/photo_stories` | Implemented | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |
| E4 | Instagram | Excluded | Excluded | Code exists but is not to be requested | Code + spec | ESTABLISHED (decision) | — | Y | — |

## F. OAuth

| # | Requirement | WhatsApp App | Flyer Lady App | Current known value | Source | Status | Who | Known before Meta? | If unknown |
|---|---|---|---|---|---|---|---|---|---|
| F1 | Flow type | JS SDK `FB.login()`, no browser redirect | Browser redirect OAuth | Both implemented | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |
| F2 | Redirect URI needed | **No** — backend endpoint, not an OAuth redirect | **Yes** | See Section 9 | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |
| F3 | Exact-match requirement | N/A | Assumed required | — | — | REQUIRES CURRENT META DOCUMENTATION VERIFICATION | Operator | N | Callback may fail |
| F4 | State handling | Session-based | Session-based, single-use | Implemented | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |

## G. Permissions

| # | Requirement | WhatsApp App | Flyer Lady App | Current known value | Source | Status | Who | Known before Meta? | If unknown |
|---|---|---|---|---|---|---|---|---|---|
| G1 | WhatsApp permission set | `whatsapp_business_messaging`, `whatsapp_business_management`, `business_management` | Must NOT hold these | In `capability_config.py` | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |
| G2 | Meta's stated required set for the WhatsApp use case | `public_profile`, `whatsapp_business_management`, `whatsapp_business_messaging` | — | **Differs from G1** | Meta doc supplied earlier | REQUIRES CURRENT META DOCUMENTATION VERIFICATION | Operator | Partial | See Section 7 discrepancy |
| G3 | Flyer Lady permission set | Must NOT hold these | `pages_show_list`, `pages_manage_posts`, (+`pages_read_engagement`?) | In `capability_config.py` | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |
| G4 | `pages_read_engagement` justification | N/A | **No direct code path** | See Correction 1 | Code | REQUIRES CURRENT META DOCUMENTATION VERIFICATION | Operator | N | May be rejected as undemonstrable, or required as a dependency |
| G5 | `business_management` for Flyer Lady | Held by App A | **UNRESOLVED** | Excluded from code | Spec §18 | CURRENTLY UNKNOWN | Operator | N | See Section 7 |
| G6 | Advanced Access needed | Yes, for real customers | Yes, for real Pages | — | — | REQUIRES CURRENT META DOCUMENTATION VERIFICATION | Operator | N | Only testers can use the apps |

## H. Webhooks

| # | Requirement | WhatsApp App | Flyer Lady App | Current known value | Source | Status | Who | Known before Meta? | If unknown |
|---|---|---|---|---|---|---|---|---|---|
| H1 | Webhook URL | `/webhooks/meta` | **None** | Implemented | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |
| H2 | Verify token | `META_WHATSAPP_WEBHOOK_VERIFY_TOKEN` (operator-chosen) | N/A | Legacy variable exists in Railway | Railway | ESTABLISHED FROM RAILWAY (name) | Operator | Partial | Handshake fails |
| H3 | Signature secret | WhatsApp App Secret only | Must never verify WhatsApp | Enforced in code | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |
| H4 | Fields to subscribe | 6 fields — see Section 11 | None | From webhook router | Code | ESTABLISHED BY VANTA BUILD | — | Y | Missing fields = silent feature loss |
| H5 | Per-customer `subscribed_apps` | Required | N/A | **Not implemented** | Code | **GAP** | Both | Y | No webhooks per customer |

## I. Data deletion

| # | Requirement | WhatsApp App | Flyer Lady App | Current known value | Source | Status | Who | Known before Meta? | If unknown |
|---|---|---|---|---|---|---|---|---|---|
| I1 | Endpoint implemented | — | `/integrations/meta/flyer-lady/data-deletion` + legacy `/data-deletion` | Both live | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |
| I2 | Whether Meta requires it per app | Unknown | Unknown | — | — | REQUIRES CURRENT META DOCUMENTATION VERIFICATION | Operator | N | App publishing may be blocked |
| I3 | Verification secret | — | Flyer Lady App Secret | Enforced | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |
| I4 | Status/confirmation URL | — | `/data-deletion/status/<code>` exists | Implemented | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |

## J. Deauthorization

| # | Requirement | WhatsApp App | Flyer Lady App | Current known value | Source | Status | Who | Known before Meta? | If unknown |
|---|---|---|---|---|---|---|---|---|---|
| J1 | Endpoint implemented | **None** | `/integrations/meta/flyer-lady/deauthorize` | Flyer Lady only | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |
| J2 | Whether Meta requires it | **UNRESOLVED** | **UNRESOLVED** | — | — | REQUIRES CURRENT META DOCUMENTATION VERIFICATION | Operator | N | See Section 13 |
| J3 | Boundary correctness | — | Never touches WhatsApp data | Structurally enforced + tested | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |

## K. App Review

| # | Requirement | WhatsApp App | Flyer Lady App | Current known value | Source | Status | Who | Known before Meta? | If unknown |
|---|---|---|---|---|---|---|---|---|---|
| K1 | Current review state | Unknown | None (app doesn't exist) | — | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | Cannot plan submission |
| K2 | Video per permission | Required | Required | — | Meta doc supplied earlier | REQUIRES CURRENT META DOCUMENTATION VERIFICATION | Operator | N | Rejection |
| K3 | Written description per permission | Required | Required | — | Meta doc supplied earlier | REQUIRES CURRENT META DOCUMENTATION VERIFICATION | Operator | N | Rejection |
| K4 | Privacy policy URL | Required | Required | Unknown whether one exists | — | OPERATOR ACTION | Operator | N | Cannot publish app |
| K5 | Terms URL | Likely required | Likely required | Unknown | — | REQUIRES LIVE META UI VERIFICATION | Operator | N | Cannot publish app |
| K6 | App icon | Required | Required | Unknown | — | OPERATOR ACTION | Operator | N | Cannot publish app |

## L. Railway / environment

| # | Requirement | WhatsApp App | Flyer Lady App | Current known value | Source | Status | Who | Known before Meta? | If unknown |
|---|---|---|---|---|---|---|---|---|---|
| L1 | Which services need Meta vars | `phanta-web`, `phanta-scheduler` | Same two | `billing` does **not** | Code | ESTABLISHED BY VANTA BUILD | — | Y | — |
| L2 | `META_WHATSAPP_*` present | Not yet | — | Absent on both services | Railway | ESTABLISHED FROM RAILWAY | Operator | Y | Legacy fallback still works |
| L3 | `META_FLYER_LADY_*` present | — | Not yet | Absent | Railway | ESTABLISHED FROM RAILWAY | Operator | Y | Flyer Lady cannot run |
| L4 | `META_FLYER_LADY_CONFIG_ID` | — | **Absent on both services** | Railway | ESTABLISHED FROM RAILWAY | Operator | Y | Flyer Lady connect returns 503 |
| L5 | Legacy vars present | Yes | Must never use them | `META_APP_ID/SECRET/...` present | Railway | ESTABLISHED FROM RAILWAY | — | Y | — |
| L6 | Railway subscription status | — | — | **Past-due banner observed** | Railway UI | OPERATOR ACTION | Operator | Y | Service disruption risk |

## M. Production URLs

| # | Requirement | Value | Source | Status |
|---|---|---|---|---|
| M1 | Production domain | `https://app.vantaautomations.co.za` | Railway custom domain on `phanta-web` | **ESTABLISHED FROM RAILWAY** |
| M2 | All callback paths | See Section 9 | Flask URL map, 101 routes | **ESTABLISHED BY VANTA BUILD** |

## N. Security

| # | Requirement | Status |
|---|---|---|
| N1 | No cross-capability credential fallback | **ESTABLISHED BY VANTA BUILD** — 55 tests |
| N2 | Flyer Lady secret rejected by WhatsApp webhook | **ESTABLISHED** — real HMAC test |
| N3 | WhatsApp secret rejected by Flyer Lady signed request | **ESTABLISHED** — real HMAC test |
| N4 | Tokens encrypted at rest | **ESTABLISHED BY VANTA BUILD** |
| N5 | Secrets never logged | **ESTABLISHED BY VANTA BUILD** — enforced by test |
| N6 | Encryption key durability | `META_TOKEN_ENCRYPTION_KEY` — losing it invalidates all stored tokens | **OPERATOR ACTION** |

## O. Testing

| # | Requirement | Status |
|---|---|---|
| O1 | Capability separation suite | 55 passed — **ESTABLISHED** |
| O2 | Failure isolation matrix | 9/9 — **ESTABLISHED** |
| O3 | App boot | 101 routes — **ESTABLISHED** |
| O4 | Regression vs baseline | Zero — **ESTABLISHED** |
| O5 | **Any real Meta API call** | **NEVER PERFORMED** — all tests use mocks or local HMAC |
| O6 | Sandbox/test account strategy | Not established | **OPERATOR ACTION** |

---

# SECTION 2 — APP A: WHATSAPP COMPLETE INFORMATION

## KNOWN FROM VANTA BUILD

| Item | Value |
|---|---|
| Webhook URL | `https://app.vantaautomations.co.za/webhooks/meta` |
| Webhook verify token variable | `META_WHATSAPP_WEBHOOK_VERIFY_TOKEN` (legacy: `META_WEBHOOK_VERIFY_TOKEN`) |
| Webhook signature secret | WhatsApp App Secret only, via `WhatsAppMetaConfig` |
| Webhook fields consumed | `messages`, `account_update`, `message_template_status_update`, `phone_number_quality_update`, `phone_number_name_update`, `security` |
| Embedded Signup backend callback | `/integrations/meta/embedded-signup/callback` — **not** an OAuth redirect URI |
| Config ID variable | `META_WHATSAPP_CONFIG_ID` (alias `META_EMBEDDED_SIGNUP_CONFIG_ID`) |
| Permission set in code | `whatsapp_business_messaging`, `whatsapp_business_management`, `business_management` |
| System User variable | `META_WHATSAPP_SYSTEM_USER_TOKEN` |
| Tech Provider variables | `META_BUSINESS_ID`, `META_SYSTEM_USER_ID`, `META_CREDIT_LINE_ID`, `META_CREDIT_SHARING_ENABLED`, `META_WABA_ASSIGNED_TASKS` |
| Graph API version default | `v26.0` |
| WABA/phone records in production DB | **0** |
| Data deletion boundary | Deletes Flyer Lady social data only; never WhatsApp business assets |

## MUST BE VERIFIED IN META

| Item | Status |
|---|---|
| Current App name | REQUIRES LIVE META UI VERIFICATION |
| App ID (value) | REQUIRES LIVE META UI VERIFICATION |
| App Secret | OPERATOR ACTION — never to be pasted anywhere |
| Business Portfolio it belongs to | REQUIRES LIVE META UI VERIFICATION |
| App mode (Development / Live) | REQUIRES LIVE META UI VERIFICATION |
| App type | REQUIRES LIVE META UI VERIFICATION |
| Use cases currently attached | REQUIRES LIVE META UI VERIFICATION |
| Products currently added | REQUIRES LIVE META UI VERIFICATION |
| WhatsApp product state | REQUIRES LIVE META UI VERIFICATION |
| Whether a Facebook Login config exists, and which variation | REQUIRES LIVE META UI VERIFICATION |
| Embedded Signup config ID (value) | REQUIRES LIVE META UI VERIFICATION |
| WABA existence and ID | REQUIRES LIVE META UI VERIFICATION |
| Phone number, its ID, and verification state | REQUIRES LIVE META UI VERIFICATION |
| Whether the webhook is currently configured, and to which URL | REQUIRES LIVE META UI VERIFICATION |
| Which webhook fields are currently subscribed | REQUIRES LIVE META UI VERIFICATION |
| System User existence and ID | REQUIRES LIVE META UI VERIFICATION |
| Credit line existence and ID | REQUIRES LIVE META UI VERIFICATION |
| Payment method on the WABA | REQUIRES LIVE META UI VERIFICATION |
| Tech Provider onboarding state | REQUIRES LIVE META UI VERIFICATION |
| Business Verification status | REQUIRES LIVE META UI VERIFICATION |
| Which permissions are currently granted, and at what access level | REQUIRES LIVE META UI VERIFICATION |
| Current App Review submissions and outcomes | REQUIRES LIVE META UI VERIFICATION |
| Privacy policy URL configured | REQUIRES LIVE META UI VERIFICATION |
| Terms URL configured | REQUIRES LIVE META UI VERIFICATION |
| Data Deletion URL currently configured | REQUIRES LIVE META UI VERIFICATION |
| Deauthorize URL currently configured | REQUIRES LIVE META UI VERIFICATION |
| Whether any Facebook Page / Flyer Lady permission is attached to App A | REQUIRES LIVE META UI VERIFICATION |
| Any active warnings, restrictions or policy notices | REQUIRES LIVE META UI VERIFICATION |

**Nothing above is guessed. No App ID, config ID, WABA ID, phone number or
secret appears in this pack.**

---

# SECTION 3 — APP A: WHAT MUST CHANGE

Determined from the completed build. **No deletion is recommended without
live verification first.**

### KEEP

- The App itself, its App ID and App Secret (§34: existing app keeps identity)
- WhatsApp product, WABA, phone number, System User, credit line
- The Embedded Signup Facebook Login configuration
- The webhook URL `/webhooks/meta`
- Business Verification and any completed App Review
- WhatsApp permissions

### REMOVE

**Nothing has been established as needing removal.**

Candidates exist only if live inspection proves them present:
- Facebook Page permissions attached to App A (`pages_*`)
- Any Instagram permission on App A
- A second Facebook Login configuration serving Flyer Lady
- A Flyer Lady OAuth redirect URI in App A's Valid OAuth Redirect URIs

**Status: REQUIRES LIVE META UI VERIFICATION.** Each must be confirmed
present, confirmed unused by WhatsApp, and confirmed not required by an
existing approved App Review, before any removal.

### DISABLE

Nothing established.

### CHANGE

| Item | From | To | Status |
|---|---|---|---|
| App name | Unknown | "VANTA Automations — WhatsApp" | Optional, cosmetic |
| Data Deletion URL | Unknown | Decision pending Section 12 | REQUIRES DOCUMENTATION VERIFICATION |

### VERIFY

- Webhook callback URL matches `/webhooks/meta` exactly
- All six webhook fields are subscribed (H4)
- Verify token in Meta matches the Railway value
- No Page/Instagram permissions attached
- The Facebook Login configuration is the Embedded Signup variation

### DO NOT TOUCH

- WABA, phone number, System User, credit line, Tech Provider config
- App ID and App Secret values
- The webhook URL, unless verification proves it wrong
- Any approved App Review permission

---

# SECTION 4 — APP A: FACEBOOK LOGIN FOR BUSINESS SETTINGS

**The field list below is HISTORICAL REFERENCE ONLY.** Meta's UI changes;
every row is marked for live verification. Do not treat this as the current
screen.

| Setting (historical name) | Expected WhatsApp value | Why | Source | Confirmed? | Live UI check |
|---|---|---|---|---|---|
| Client OAuth Login | Enabled | Embedded Signup uses OAuth | Historical | No | **REQUIRED** |
| Web OAuth Login | Enabled | Browser-based flow | Historical | No | **REQUIRED** |
| Force Web OAuth Reauthentication | Unknown | Not established | — | No | **REQUIRED** |
| Enforce HTTPS | Enabled | Production is HTTPS-only | Historical | No | **REQUIRED** |
| Embedded Browser OAuth Login | Enabled | Signup runs in a popup | Historical | No | **REQUIRED** |
| Strict Mode for Redirect URIs | Enabled | Security | Historical | No | **REQUIRED** |
| Valid OAuth Redirect URIs | **Possibly empty** — see note | JS SDK flow may need none | Code | No | **REQUIRED** |
| Login from Devices | Unknown | Not used | — | No | **REQUIRED** |
| Login with JavaScript SDK | Enabled | `FB.login()` is the mechanism | Code | Partially | **REQUIRED** |
| Allowed Domains for JavaScript SDK | `app.vantaautomations.co.za` | JS SDK origin | Code + Railway | Partially | **REQUIRED** |
| Deauthorize Callback URL | Unresolved | See Section 13 | — | No | **REQUIRED** |
| Data Deletion Request URL | Unresolved | See Section 12 | — | No | **REQUIRED** |
| Redirect URI Validator | Tool, not a setting | — | Historical | No | **REQUIRED** |

**Note on Valid OAuth Redirect URIs for App A:** VANTA's Embedded Signup
uses the JavaScript SDK and returns an authorization code to the page, then
POSTs it to a backend endpoint. `/integrations/meta/embedded-signup/callback`
is **not** a browser redirect target. Whether Meta nonetheless requires a
redirect URI entry for this configuration is
**REQUIRES CURRENT META DOCUMENTATION VERIFICATION.** Do not add one merely
to make App A resemble App B.

---

# SECTION 5 — APP A: WHATSAPP EMBEDDED SIGNUP

## CODE ALREADY IMPLEMENTED

| Capability | Location |
|---|---|
| JS SDK launch with `config_id` | `static/meta/embedded_signup.js` |
| Session/nonce creation, 15-min TTL | `embedded_signup_service.py` |
| `WA_EMBEDDED_SIGNUP` message listener | `static/meta/embedded_signup.js` |
| Authorization-code POST to backend | `/integrations/meta/embedded-signup/callback` |
| Server-side code → token exchange | `embedded_signup_service.py` |
| Token encryption at rest | `MetaTokenStore` |
| WABA/phone persistence | `MetaBusinessConnection` |
| Webhook receipt + signature verification | `routes/webhooks.py` |
| Phone registration endpoints | `/phone/register`, `/request-code`, `/verify-code`, `/pin` |
| Tech Provider onboarding service | `tech_provider_onboarding_service.py` |

## META MUST BE CONFIGURED

- Facebook Login for Business configuration, Embedded Signup variation
- Allowed domains for the JavaScript SDK
- Webhook callback URL and verify token
- Webhook field subscriptions
- Payment method on the WABA — **OPERATOR ACTION**, and per Meta's Tech
  Provider model this is the **customer's** action for each onboarded
  business, not VANTA's

## META MUST BE VERIFIED

- The configuration ID in Railway matches the live configuration
- The configuration's permission set matches G1
- Advanced Access status for each WhatsApp permission
- Business Verification complete
- App mode and its effect on who can complete signup

## HUMAN OPERATOR ACTION

- Supply the configuration ID value
- Complete Business Verification
- Complete App Review with per-permission video and description
- Add the phone number and complete its verification

## TWO IMPLEMENTATION GAPS THAT BLOCK A WORKING SIGNUP

These are **code gaps**, established by reading the codebase, and no amount
of Meta configuration fixes them:

**GAP 1 — `POST /<WABA_ID>/subscribed_apps` is never called.**
Verified: `grep -rn "subscribed_apps"` returns nothing in application code.
Per Meta's Tech Provider documentation, without this call Meta delivers
**no webhook events at all** for that business customer, regardless of how
correctly the app-level webhook is configured.

**GAP 2 — phone PIN registration has no caller.**
`routes/meta.py` exposes `/phone/register`, `/phone/request-code`,
`/phone/verify-code`, `/phone/pin`. Verified: no template or script calls
any of them. Whether Meta's signup popup performs this internally for a
new number is **REQUIRES CURRENT META DOCUMENTATION VERIFICATION**.

**Neither gap is in scope for the App Separation build.** Both must be
decided before a real workshop is onboarded.

---

# SECTION 6 — APP B: FLYER LADY COMPLETE INFORMATION

## ESTABLISHED BY VANTA BUILD

| Item | Value |
|---|---|
| Target name | VANTA Automations — Flyer Lady |
| Portfolio | Same as App A — **REQUIRES LIVE VERIFICATION** that A1 holds |
| OAuth callback | `https://app.vantaautomations.co.za/dashboard/flyer-lady/connect/callback` |
| Data deletion callback | `https://app.vantaautomations.co.za/integrations/meta/flyer-lady/data-deletion` |
| Deauthorization callback | `https://app.vantaautomations.co.za/integrations/meta/flyer-lady/deauthorize` |
| Flow | Browser redirect OAuth — **not** the JS SDK |
| Graph operations | `GET /me/accounts`; `POST /{page_id}/photos`; `POST /{page_id}/photo_stories` |
| Token handling | Short-lived → long-lived user token → Page token, encrypted at rest |
| Models | `MetaSocialConnection`, `MetaSocialOAuthSession` |
| Existing connections | **0** — clean creation, nothing to migrate |
| Instagram | Code present, deliberately **not** to be requested |
| Webhooks | **None** |
| System User | **None** — structurally impossible |

## MUST BE ESTABLISHED BEFORE CREATION

| Item | Status |
|---|---|
| App type to choose at creation | REQUIRES LIVE META UI VERIFICATION |
| Which use case (if the creation flow demands one) | REQUIRES LIVE META UI VERIFICATION |
| Whether FLB must be added as a product separately | REQUIRES LIVE META UI VERIFICATION |
| Login configuration variation name | REQUIRES LIVE META UI VERIFICATION |
| Privacy policy URL | OPERATOR ACTION |
| Terms URL | OPERATOR ACTION |
| App icon | OPERATOR ACTION |
| Which Facebook Page will be used for the App Review demo | OPERATOR ACTION |
| Final permission set | See Section 7 — **partly unresolved** |

**App ID, App Secret and configuration ID do not exist yet and are not
invented anywhere in this pack.**

---

# SECTION 7 — FLYER LADY PERMISSIONS

## `pages_show_list`

| Question | Answer |
|---|---|
| What VANTA uses it for | Listing the Pages a connecting user administers |
| Exact code operation | `MetaSocialGraphClient.list_pages` → `GET /me/accounts` |
| Demonstrably required? | **Yes** — without it, Page selection is impossible |
| Meta documentation supports it? | REQUIRES CURRENT META DOCUMENTATION VERIFICATION |
| App Review required? | REQUIRES CURRENT META DOCUMENTATION VERIFICATION |
| Advanced Access required? | Yes, for non-tester users — VERIFY |
| Dependency on another permission? | VERIFY |
| **Should it be requested?** | **YES** |

## `pages_manage_posts`

| Question | Answer |
|---|---|
| What VANTA uses it for | Publishing Page feed photos and Page Stories |
| Exact code operation | `POST /{page_id}/photos`; `POST /{page_id}/photo_stories` |
| Demonstrably required? | **Yes** — this is the entire feature |
| Meta documentation supports it? | REQUIRES CURRENT META DOCUMENTATION VERIFICATION |
| App Review required? | REQUIRES CURRENT META DOCUMENTATION VERIFICATION |
| Advanced Access required? | Yes, for real Pages — VERIFY |
| Dependency on another permission? | **Historically `pages_read_engagement`** — VERIFY |
| **Should it be requested?** | **YES** |

## `pages_read_engagement` — **status changed by Correction 1**

| Question | Answer |
|---|---|
| What VANTA uses it for | **Nothing directly.** No code path reads Page engagement or content. |
| Exact code operation | **NONE FOUND** |
| Demonstrably required? | **NO — not on its own merits** |
| Meta documentation supports it? | Only if it is a stated dependency of `pages_manage_posts` |
| App Review required? | REQUIRES CURRENT META DOCUMENTATION VERIFICATION |
| **Should it be requested?** | **CONDITIONAL — resolve before submission** |

**The decision rule, stated precisely:**

- If current Meta documentation states `pages_manage_posts` **requires**
  `pages_read_engagement` → request it, and justify it in App Review as a
  documented dependency, not as a feature.
- If it does **not** → **remove it**, because §49 says an undemonstrable
  permission risks rejection, and there is no screencast that could
  legitimately show VANTA using it.

**Status: REQUIRES CURRENT META DOCUMENTATION VERIFICATION. Do not submit
App Review until resolved.**

## `business_management` — **UNRESOLVED, as the spec requires**

| Question | Answer |
|---|---|
| What VANTA would use it for | **Nothing in the Flyer Lady flow.** The Page token comes from `/me/accounts`, not from business-asset management. |
| Exact code operation | **NONE** in `flyer_lady/` or `integrations/meta/social/` |
| Currently in the code's permission set? | **No — deliberately excluded** |
| Held by App A? | Yes — for Tech Provider WABA/System User assignment, a genuine use |
| Demonstrably required for Flyer Lady? | **No evidence found** |
| **Recommendation** | **Do NOT request it** unless live testing of the Facebook Login for Business flow proves the authorization fails without it |

**Why this is genuinely uncertain:** Facebook Login *for Business* (as
distinct from classic Facebook Login) issues business-scoped tokens, and
some configurations require a business-management context. Whether that
manifests as a required `business_management` permission for this exact
flow cannot be established from the code.

**Resolution method:** create the app, configure the login with only
`pages_show_list` + `pages_manage_posts` (+ `pages_read_engagement` if the
dependency is confirmed), and run the real flow in Development Mode. If
`/me/accounts` returns the Pages and publishing succeeds,
`business_management` is not required. **REQUIRES LIVE META UI VERIFICATION.**

## PERMISSIONS THAT MUST NOT BE REQUESTED

| Permission | Reason | Overriding condition |
|---|---|---|
| `pages_manage_metadata` | No code path | Only if docs prove it required |
| `pages_manage_engagement` | No code path | Only if docs prove it required |
| `pages_read_user_content` | No code path | Only if docs prove it required |
| `read_insights` | No code path | Only if docs prove it required |
| `pages_messaging` | No code path; Flyer Lady does not message | Never for this flow |
| `instagram_basic` | Instagram deliberately out of scope | Only when Instagram is implemented and submitted |
| `instagram_content_publish` | Same | Same |

**Open risk on Instagram (Correction 2):** `list_pages` requests the
`instagram_business_account` field. Behaviour without Instagram permissions
is **REQUIRES LIVE VERIFICATION**.

## DISCREPANCY: WhatsApp permission sets do not match

| Source | Set |
|---|---|
| VANTA code (`WHATSAPP_REQUIRED_PERMISSIONS`) | `whatsapp_business_messaging`, `whatsapp_business_management`, `business_management` |
| Meta's WhatsApp use-case doc (supplied earlier) | `public_profile`, `whatsapp_business_management`, `whatsapp_business_messaging` |

VANTA requires `business_management` (which Meta's doc calls *optional* for
the use case) and does not check `public_profile` (which Meta calls
*required*). `business_management` is genuinely used by Tech Provider
operations, so requiring it is defensible; `public_profile` is typically
granted automatically. **Status: REQUIRES CURRENT META DOCUMENTATION
VERIFICATION.** No code change recommended until verified.

---

# SECTION 8 — APP B: FACEBOOK LOGIN FOR BUSINESS

## The implemented flow

```
User clicks Connect
  -> GET /dashboard/flyer-lady/connect/start
     - builds https://www.facebook.com/<version>/dialog/oauth
     - client_id     = Flyer Lady App ID
     - config_id     = Flyer Lady FLB configuration ID
     - redirect_uri  = exact registered URI
     - state         = server-side, single-use
     - scope         = from FlyerLadyMetaConfig.required_permissions
  -> Meta authorization
  -> GET /dashboard/flyer-lady/connect/callback?code=...&state=...
     - verifies state
     - exchanges code (Flyer Lady App ID + Secret + identical redirect_uri)
     - exchanges for long-lived user token
     - GET /me -> Meta user id
     - GET /me/accounts -> Pages
  -> page picker rendered
  -> POST /dashboard/flyer-lady/connect/complete
     - stores encrypted Page token in MetaSocialConnection
```

**ESTABLISHED BY VANTA BUILD.**

## WhatsApp login requirements vs Flyer Lady login requirements — never merge

| Aspect | App A (WhatsApp) | App B (Flyer Lady) |
|---|---|---|
| Mechanism | JavaScript SDK `FB.login()` | Full-page browser redirect |
| Login variation | WhatsApp Embedded Signup | Standard Facebook Login |
| Redirect URI | Not used as a browser target | **Required, exact match** |
| JS SDK domains | Required | Not required |
| Returns | WABA ID, phone number ID, code | Authorization code only |
| Permissions | WhatsApp scopes | Page scopes |
| Webhooks | Yes | **No** |

**Do not copy App A's settings into App B.**

## Must be verified in the live UI

- Whether FLB is added as a product or is implicit
- The exact login-variation names currently offered
- Whether the configuration itself pins permissions, or the `scope`
  parameter governs
- Whether `config_id` is mandatory for a standard (non-Embedded-Signup)
  configuration — **the code always sends it**
- Where Valid OAuth Redirect URIs are entered for this app

---

# SECTION 9 — REDIRECT URI REQUIREMENTS

| # | URL | Meta setting it belongs to | Exact match? | HTTPS | Query allowed? | Fragment? | Required? |
|---|---|---|---|---|---|---|---|
| 1 | `/integrations/meta/embedded-signup/callback` | **Probably none** — backend endpoint, not a browser redirect | N/A | Yes | N/A | N/A | **VERIFY** |
| 2 | `/webhooks/meta` | WhatsApp → Webhooks → Callback URL | Yes | Yes | No | No | **Yes** |
| 3 | `/dashboard/flyer-lady/connect/callback` | Flyer Lady FLB → Valid OAuth Redirect URIs | **Yes** | **Yes** | **No** | **No** | **Yes** |
| 4 | `/integrations/meta/flyer-lady/data-deletion` | Flyer Lady → Data Deletion Request URL | Yes | Yes | No | No | **VERIFY** |
| 5 | `/integrations/meta/flyer-lady/deauthorize` | Flyer Lady → Deauthorize Callback URL | Yes | Yes | No | No | **VERIFY** |
| 6 | `/data-deletion` | Possibly already set on App A | Yes | Yes | No | No | **VERIFY — do not remove blindly** |

All six paths are **ESTABLISHED BY VANTA BUILD** — confirmed present in the
Flask URL map (101 routes).

**#3 is enforced in code:** `FlyerLadyMetaConfig.validate_oauth()` rejects a
redirect URI that is not HTTPS or that carries a query string or fragment,
because the identical value must be reusable verbatim in the code exchange.

**#6 warning:** `/data-deletion` may already be registered on App A in
Meta. Removing it could break a live callback. Verify before changing.

Exact-match, query and fragment rules are marked
**REQUIRES CURRENT META DOCUMENTATION VERIFICATION**; the code enforces the
stricter interpretation regardless.

---

# SECTION 10 — DOMAINS

**Domain configuration is not callback configuration.** They are separate
Meta settings that happen to reference the same host.

| Setting | App A | App B | Value | Status |
|---|---|---|---|---|
| App Domains | Likely required | Likely required | `app.vantaautomations.co.za` | REQUIRES LIVE META UI VERIFICATION |
| Allowed Domains for JavaScript SDK | **Required** — JS SDK flow | **Not required** — no JS SDK | `app.vantaautomations.co.za` | REQUIRES LIVE META UI VERIFICATION |
| Valid OAuth Redirect URIs | Probably none | **Required** — full URL #3 | See Section 9 | REQUIRES LIVE META UI VERIFICATION |

**VANTA-side variables:** `META_WHATSAPP_APP_DOMAINS` and
`META_FLYER_LADY_APP_DOMAINS` are separate. Both may hold the same value —
**a shared domain is not a shared Meta App identity.**
`FlyerLadyMetaConfig` validates that each entry is a well-formed HTTPS URL
with no credentials, query or fragment. **ESTABLISHED BY VANTA BUILD.**

---

# SECTION 11 — WEBHOOKS

## Ownership

**The WhatsApp App owns the webhook. Flyer Lady has none.**
**ESTABLISHED BY VANTA BUILD** — `routes/webhooks.py` resolves its secret
and verify token exclusively through `WhatsAppMetaConfig`, and no Flyer Lady
code path processes webhooks.

## Configuration

| Item | Value | Status |
|---|---|---|
| Callback URL | `https://app.vantaautomations.co.za/webhooks/meta` | ESTABLISHED |
| Verify token | `META_WHATSAPP_WEBHOOK_VERIFY_TOKEN` — operator-chosen, not issued by Meta | ESTABLISHED |
| Signature secret | WhatsApp App Secret, via `WhatsAppMetaConfig` | ESTABLISHED |
| Signature algorithm | `X-Hub-Signature-256`, HMAC-SHA256, constant-time compare | ESTABLISHED |

## Fields the code actually consumes

| Field | Handler |
|---|---|
| `messages` | inbound messages, delivery status |
| `account_update` | WABA account changes |
| `message_template_status_update` | **template approval sync — without this, approved templates never become usable** |
| `phone_number_quality_update` | quality rating |
| `phone_number_name_update` | display-name approval |
| `security` | security events |

**ESTABLISHED BY VANTA BUILD** — read from `webhook_router.py`. Subscribing
to fewer means silent feature loss, not an error.

## Verification behaviour

`GET /webhooks/meta` returns **503** with the missing variable name when the
verify token is unconfigured, rather than a 500 — so an unconfigured
deployment is distinguishable from a broken one. **ESTABLISHED.**

## Per-customer subscription — GAP

`POST /<WABA_ID>/subscribed_apps` after each Embedded Signup is **not
implemented**. App-level webhook configuration alone does not deliver
events for an onboarded business customer.
**REQUIRES CURRENT META DOCUMENTATION VERIFICATION** to confirm current
requirement; **code change required** if confirmed.

## Does App Review affect webhooks?

**REQUIRES CURRENT META DOCUMENTATION VERIFICATION.**

## Does Flyer Lady need a webhook?

**No — not for the implemented flow.** Publishing is outbound only. A
webhook would only be needed for comment/mention/DM listening, which is not
implemented and out of scope.

---

# SECTION 12 — DATA DELETION

**Do not assume the two apps have identical requirements.**

## App A — WhatsApp

| Question | Answer |
|---|---|
| Is a Data Deletion Request URL required? | **REQUIRES CURRENT META DOCUMENTATION VERIFICATION** |
| Is a callback required? | UNRESOLVED |
| Is one currently configured? | **REQUIRES LIVE META UI VERIFICATION** |
| What would VANTA delete? | **Nothing WhatsApp-specific is implemented.** |

**The honest position:** VANTA's only data-deletion implementation deletes
Flyer Lady social data. If Meta requires a deletion callback on the WhatsApp
app, VANTA must decide what WhatsApp-owned data a *personal Facebook user*
deletion request should affect — and the existing code comment argues
correctly that WABA/System User credentials are **business assets, not the
callback user's personal data**. That reasoning has not been validated
against Meta policy. **UNRESOLVED.**

## App B — Flyer Lady

| Question | Answer | Status |
|---|---|---|
| Required? | Likely, as it handles Facebook user data | REQUIRES DOCUMENTATION VERIFICATION |
| Endpoint | `/integrations/meta/flyer-lady/data-deletion` | ESTABLISHED |
| Legacy endpoint | `/data-deletion` — retained | ESTABLISHED |
| `signed_request` parsing | HMAC-SHA256, constant-time, `algorithm` field required | ESTABLISHED |
| App Secret used | **Flyer Lady only** | ESTABLISHED |
| Status URL | `/data-deletion/status/<confirmation_code>` | ESTABLISHED |
| Response format Meta expects | **REQUIRES DOCUMENTATION VERIFICATION** | — |

### What VANTA deletes

- `MetaSocialConnection` rows for that Meta user
- `MetaSocialOAuthSession` rows for the affected locations
- Writes an audit entry per removal

### What VANTA deliberately does NOT delete

- WABA, phone numbers, System User credentials
- WhatsApp billing, credit-line state, message history
- The workshop's specials, posts and click history — VANTA business data,
  not the user's personal Facebook data

**Structurally enforced:** `delete_meta_user_data()` never references any
WhatsApp-owned model. Asserted by test against the function's own source.

---

# SECTION 13 — DEAUTHORIZATION

**Deliberately unresolved. Not invented here.**

## Does the WhatsApp App require a deauthorization callback?

**CURRENTLY UNKNOWN — REQUIRES CURRENT META DOCUMENTATION VERIFICATION.**

Not implemented. No endpoint exists at
`/integrations/meta/whatsapp/deauthorize`.

If verification proves it required, the implementation must invert the
Flyer Lady boundary: act only on WhatsApp-owned records, never on
`MetaSocialConnection`. **Do not build it on assumption.**

## Does the Flyer Lady App require one?

**REQUIRES CURRENT META DOCUMENTATION VERIFICATION** — but it is
**already implemented**, so this is lower risk. If not required, the
endpoint is harmless; if required, it is ready.

## What Meta expects, if required

**REQUIRES CURRENT META DOCUMENTATION VERIFICATION.** VANTA's implementation
assumes the same `signed_request` mechanism as data deletion. That
assumption is not verified.

## Capability boundary — ESTABLISHED

Flyer Lady deauthorization **never** deletes WhatsApp business data.
`deauthorize_flyer_lady_user()` queries only `MetaSocialConnection` and
`MetaSocialOAuthSession`. A test asserts no WhatsApp model or term appears
in its code. It clears the Page token and marks the connection `revoked`
rather than deleting the row, so the workshop's own content survives and a
reconnecting user finds their specials intact.

**There is no global "delete all Meta data for this user" operation.**

---

# SECTION 14 — SECURITY BOUNDARIES: FINAL CREDENTIAL MATRIX

## Permitted use

| Variable | WhatsApp | Flyer Lady |
|---|---|---|
| `META_WHATSAPP_APP_ID` | ✅ | ❌ |
| `META_WHATSAPP_APP_SECRET` | ✅ | ❌ |
| `META_WHATSAPP_GRAPH_API_VERSION` | ✅ | ❌ |
| `META_WHATSAPP_APP_DOMAINS` | ✅ | ❌ |
| `META_WHATSAPP_CONFIG_ID` | ✅ | ❌ |
| `META_WHATSAPP_SYSTEM_USER_TOKEN` | ✅ | ❌ **structurally impossible** |
| `META_WHATSAPP_WEBHOOK_VERIFY_TOKEN` | ✅ | ❌ |
| `META_FLYER_LADY_APP_ID` | ❌ | ✅ |
| `META_FLYER_LADY_APP_SECRET` | ❌ | ✅ |
| `META_FLYER_LADY_GRAPH_API_VERSION` | ❌ | ✅ |
| `META_FLYER_LADY_APP_DOMAINS` | ❌ | ✅ |
| `META_FLYER_LADY_CONFIG_ID` | ❌ | ✅ |
| `META_FLYER_LADY_OAUTH_REDIRECT_URI` | ❌ | ✅ |

## Legacy variables — which remain, and why

| Variable | Read by WhatsApp? | Read by Flyer Lady? | Why it remains |
|---|---|---|---|
| `META_APP_ID` | ✅ transitional | ❌ **never** | Existing production app keeps its identity |
| `META_APP_SECRET` | ✅ transitional | ❌ **never** | Same |
| `META_GRAPH_API_VERSION` | ✅ transitional | ❌ **never** | Same |
| `META_APP_DOMAINS` | ✅ transitional | ❌ **never** | Same |
| `META_SYSTEM_USER_TOKEN` | ✅ transitional | ❌ **never** | Same |
| `META_WEBHOOK_VERIFY_TOKEN` | ✅ transitional | ❌ **never** | Same |
| `META_EMBEDDED_SIGNUP_CONFIG_ID` | ✅ alias | ❌ **never** | Pre-existing alias |
| `META_SOCIAL_REDIRECT_URI` | ❌ | ⚠️ **fallback only** | Transitional until `META_FLYER_LADY_OAUTH_REDIRECT_URI` is set |

**Why the asymmetry is correct:** the legacy credentials *are* the existing
WhatsApp app's credentials, so WhatsApp reading them is accurate. The Flyer
Lady app is new — no legacy value could belong to it. A fallback there
would mean Flyer Lady silently authenticating as the WhatsApp app whenever
its own variables were missing: invisible in code review, invisible at
startup, surfacing only when Meta rejected a live call.

**Note on `META_SOCIAL_REDIRECT_URI`:** this is the one Flyer Lady legacy
touch-point. It supplies only a URL, never a credential, and is ordered
*after* `META_FLYER_LADY_OAUTH_REDIRECT_URI`. Not a credential leak.
**ESTABLISHED BY VANTA BUILD.**

## Enforced and proven

- No cross-capability fallback anywhere (55 tests)
- Flyer Lady secret rejected by WhatsApp webhook — real HMAC
- WhatsApp secret rejected by Flyer Lady signed request — real HMAC
- `FlyerLadyMetaConfig` has no `system_user_token` field at all
- `GraphApiClient` imports neither config — cannot choose credentials
- Every error names its own capability's variable

## Encryption

`META_TOKEN_ENCRYPTION_KEY` encrypts Meta tokens, Paystack card
authorizations and Google refresh tokens. **Losing or rotating it makes all
stored tokens permanently unreadable.** A normal redeploy never touches it;
only editing the variable does. **OPERATOR ACTION: store it durably outside
Railway.**

---

# SECTION 15 — RAILWAY REQUIREMENTS

**Verified against the live Railway project. Values never displayed.**

## Current state

| Service | Meta variables present now |
|---|---|
| `phanta-web` | `META_APP_DOMAINS`, `META_APP_ID`, `META_APP_SECRET`, `META_EMBEDDED_SIGNUP_CONFIG_ID`, `META_GRAPH_API_VERSION`, `META_SYSTEM_USER_TOKEN`, `META_TOKEN_ENCRYPTION_KEY`, `META_WEBHOOK_VERIFY_TOKEN`, `META_WHATSAPP_CONFIG_ID` |
| `phanta-scheduler` | Same set |
| `billing` | `META_APP_DOMAINS` only — **vestigial, not needed** |

**Two observations, both ESTABLISHED FROM RAILWAY:**

1. **`META_FLYER_LADY_CONFIG_ID` is absent from both services.** Under the
   pre-separation code this made the Flyer Lady connect route return 503.
2. **`billing` does not need Meta variables** — verified against the code.

## WHATSAPP variables

| Variable | Purpose | Required? | Source | Exists? | Create when | Remove when |
|---|---|---|---|---|---|---|
| `META_WHATSAPP_APP_ID` | App A identity | Required | Copy of `META_APP_ID` | No | Now | Never |
| `META_WHATSAPP_APP_SECRET` | Token exchange + webhook signature | Required | Copy of `META_APP_SECRET` | No | Now | Never |
| `META_WHATSAPP_GRAPH_API_VERSION` | API version | Optional (`v26.0`) | Copy | No | Now | Never |
| `META_WHATSAPP_APP_DOMAINS` | Embedded Signup validation | Conditional | Copy | No | Now | Never |
| `META_WHATSAPP_CONFIG_ID` | Embedded Signup config | Conditional | Already set | **Yes** | — | Never |
| `META_WHATSAPP_SYSTEM_USER_TOKEN` | System User ops | Conditional | Copy | No | Now | Never |
| `META_WHATSAPP_WEBHOOK_VERIFY_TOKEN` | Webhook handshake | Conditional | Copy | No | Now | Never |

Set on `phanta-web` and `phanta-scheduler`. **Not** on `billing`.

## FLYER LADY variables

| Variable | Purpose | Required? | Source | Exists? | Create when | Remove when |
|---|---|---|---|---|---|---|
| `META_FLYER_LADY_APP_ID` | App B identity | Required | New app | No | After §52 Step 8 | Never |
| `META_FLYER_LADY_APP_SECRET` | Token exchange, deletion, deauth | Required | New app | No | After Step 8 | Never |
| `META_FLYER_LADY_GRAPH_API_VERSION` | API version | Optional | — | No | Optional | Never |
| `META_FLYER_LADY_APP_DOMAINS` | Domain validation | Conditional | — | No | After Step 8 | Never |
| `META_FLYER_LADY_CONFIG_ID` | FLB config | Required | New config | **No** | After Step 9 | Never |
| `META_FLYER_LADY_OAUTH_REDIRECT_URI` | Exact-match OAuth URI | Required | Known value | No | Now | Never |

The redirect URI value is already known:
`https://app.vantaautomations.co.za/dashboard/flyer-lady/connect/callback`

## LEGACY TRANSITIONAL

| Variable | Exists? | Remove when |
|---|---|---|
| `META_APP_ID` | Yes | After `META_WHATSAPP_APP_ID` verified |
| `META_APP_SECRET` | Yes | After `META_WHATSAPP_APP_SECRET` verified |
| `META_GRAPH_API_VERSION` | Yes | After WhatsApp equivalent set |
| `META_APP_DOMAINS` | Yes (all 3) | After WhatsApp equivalent set |
| `META_SYSTEM_USER_TOKEN` | Yes | After WhatsApp equivalent set |
| `META_WEBHOOK_VERIFY_TOKEN` | Yes | After WhatsApp equivalent set |
| `META_EMBEDDED_SIGNUP_CONFIG_ID` | Yes | After confirming `META_WHATSAPP_CONFIG_ID` is authoritative |
| `META_SOCIAL_REDIRECT_URI` | Unknown | After `META_FLYER_LADY_OAUTH_REDIRECT_URI` set |

`scripts/check_env.py` warns while any legacy variable remains.

## NOT a Meta variable but operationally critical

| Variable | Note |
|---|---|
| `META_TOKEN_ENCRYPTION_KEY` | Despite the prefix, capability-agnostic. **Never rotate casually.** |

## Separate operational risk

**Railway shows a past-due subscription warning** with a service-disruption
notice. **OPERATOR ACTION** — unrelated to Meta, but it would halt
everything.

---

# SECTION 16 — APP REVIEW CHECKLISTS

# ⚠️ DO NOT MIX THE TWO APP REVIEW VIDEOS ⚠️

Meta rejects submissions that combine permissions into one video. Each
permission needs its **own** clip **and** its own written description.
**REQUIRES CURRENT META DOCUMENTATION VERIFICATION** for exact current form.

## APP A — WhatsApp

**Permissions to submit**
- [ ] `whatsapp_business_messaging`
- [ ] `whatsapp_business_management`
- [ ] `business_management` — justified by Tech Provider WABA/System User assignment
- [ ] Confirm whether `public_profile` must be submitted — see §7 discrepancy

**Prerequisites**
- [ ] Business Verification complete
- [ ] App icon, privacy policy URL, app category
- [ ] Terms URL — VERIFY whether required
- [ ] Data Deletion URL — VERIFY whether required (§12)
- [ ] Deauthorize URL — VERIFY whether required (§13)

**Screencasts — one per permission**
- [ ] Embedded Signup opens from within VANTA
- [ ] A business authorizes and selects its WABA
- [ ] WABA/phone onboarding completes
- [ ] A real message is sent and appears in WhatsApp
- [ ] A template is created or managed

**Written descriptions**
- [ ] One per permission: what it does, why it is necessary, what value it adds

**Test instructions**
- [ ] Step-by-step for a Meta reviewer, including test credentials

**Blockers to resolve first**
- [ ] **GAP 1** — `subscribed_apps` not called (§5)
- [ ] **GAP 2** — phone PIN registration has no UI caller (§5)
- [ ] Message templates created and approved in WhatsApp Manager

## APP B — Flyer Lady

**Permissions to submit**
- [ ] `pages_show_list`
- [ ] `pages_manage_posts`
- [ ] `pages_read_engagement` — **only if §7 resolves as required**
- [ ] `business_management` — **only if live testing proves it required**

**Prerequisites**
- [ ] App exists and is in the same Business Portfolio
- [ ] App icon, privacy policy URL, app category
- [ ] Terms URL — VERIFY
- [ ] Data Deletion URL configured (§12)
- [ ] Deauthorize URL configured (§13)
- [ ] A real Facebook Page available for the demo

**Screencasts — one per permission**
- [ ] Connect Facebook from within VANTA
- [ ] Facebook Login for Business permission grant
- [ ] Page selection screen
- [ ] VANTA confirms the Page is connected
- [ ] A special is published to the Page feed → **visible on the real Page**
- [ ] A Story is published → **visible on the real Page**

**Written descriptions**
- [ ] One per permission

**Must NOT appear in the Flyer Lady submission**
- [ ] No WhatsApp footage
- [ ] No Instagram permissions or footage
- [ ] No permission without a corresponding clip

---

# SECTION 17 — META BUSINESS PORTFOLIO

Every item: **REQUIRES LIVE META UI VERIFICATION.**

| # | Item | What to establish | Risk if unverified |
|---|---|---|---|
| 1 | Portfolio identity | Exact name and ID | Apps created in the wrong portfolio |
| 2 | Ownership | Operator is admin | Cannot configure |
| 3 | App A association | Which portfolio owns App A | Wrong portfolio for App B |
| 4 | Business Verification | Current status and date | Blocks Advanced Access |
| 5 | Pages owned | Which Pages, and by which portfolio | Page selection returns nothing |
| 6 | WABAs owned | Which, and their state | Risk of touching the wrong WABA |
| 7 | System Users | Which exist, their roles, assigned assets | Tech Provider ops fail |
| 8 | Assets | Full inventory | Unknown dependencies |
| 9 | Permissions/roles | Who can do what | Access failures mid-configuration |
| 10 | Tech Provider status | Whether onboarding completed | Cannot onboard customers |
| 11 | Credit line | Existence, ID, currency | Billing model may be impossible (§D8) |

**Rule:** do not assume an asset belongs to this portfolio because the name
looks right. Confirm the owning portfolio ID for every asset before use.

---

# SECTION 18 — EXISTING PORTFOLIOS

**No deletion is recommended. No portfolio is recommended for
consolidation.** Nothing here can be established from code or Railway.

## Must be protected until proven safe

| Asset | Why | Action |
|---|---|---|
| Existing WhatsApp WABA | Carries messaging history, quality rating, templates | **DO NOT DELETE** |
| Existing VANTA Facebook Page | Target of Flyer Lady publishing | **DO NOT DELETE** |
| Existing production WhatsApp number | Registering elsewhere may require full re-verification | **DO NOT MOVE** |
| Existing Tech Provider configuration | Rebuilding may require re-review | **DO NOT RECREATE** |
| Existing System User | Holds asset assignments | **DO NOT DELETE** |
| Existing credit line | Tied to billing | **DO NOT DELETE** |

## Method before touching any older portfolio

1. Inventory every asset it owns — **LIVE UI**
2. For each, determine whether anything in production depends on it
3. For WABAs/numbers, determine whether messaging is live
4. Only then consider consolidation

**Deleting a portfolio can orphan a WABA or force a phone number through
full re-verification. Neither is reversible on a deadline.**

**Note:** VANTA's own database has **0** WhatsApp connections and **0**
Flyer Lady connections, so *VANTA* depends on none of these today. That
does **not** mean Meta-side assets are safe to delete — Meta-side state is
independent of VANTA's database.

---

# SECTION 19 — META UI WALKTHROUGH METHODOLOGY

**No assumed steps. No "click X and Meta will show Y."** This is a
methodology, executed one screen at a time.

## The loop — non-negotiable

```
OPERATOR OPENS ONE META SCREEN
        ↓
OPERATOR SHARES THE ACTUAL SCREEN
        ↓
ANALYZE WHAT IS ACTUALLY SHOWN
        ↓
COMPARE AGAINST THIS PACK
        ↓
DECIDE — or record what is still unknown
        ↓
CHANGE (only if the decision is clear)
        ↓
VERIFY THE CHANGE ON SCREEN
        ↓
MOVE TO THE NEXT SCREEN
```

**Stop after every screen.** Do not batch. Do not predict the next screen.

## APP A — inspection order

### A-PAGE 1: App Dashboard (landing)
- **Inspect:** app name, App ID, mode, type, products, use cases, warnings
- **Need:** whether Flyer Lady/Page functionality is attached
- **Can decide:** whether §3 REMOVE candidates exist
- **Cannot decide:** anything about App B

### A-PAGE 2: Settings → Basic
- **Inspect:** App Domains, privacy policy, terms, category, icon, data deletion URL, DPO
- **Need:** whether `/data-deletion` is registered here
- **Can decide:** whether App Review prerequisites are met
- **Cannot decide:** whether Meta requires the deletion URL (needs docs)

### A-PAGE 3: Facebook Login for Business → Settings
- **Inspect:** every field currently presented (§4 list is historical only)
- **Need:** actual field names, actual values
- **Can decide:** whether JS SDK settings match the code
- **Cannot decide:** whether App A needs a Valid OAuth Redirect URI

### A-PAGE 4: Facebook Login for Business → Configurations
- **Inspect:** every configuration, its variation, its ID, its permissions
- **Need:** whether the Railway `META_WHATSAPP_CONFIG_ID` matches; whether a Flyer Lady config exists here
- **Can decide:** whether App A holds a configuration it should not
- **Cannot decide:** removal, until dependency is proven

### A-PAGE 5: WhatsApp → Configuration / API Setup
- **Inspect:** WABA, phone numbers, IDs, webhook URL, subscribed fields
- **Need:** whether all six fields (§11) are subscribed
- **Can decide:** which fields to add
- **Cannot decide:** whether GAP 1 is closed — that is code

### A-PAGE 6: Permissions and Features
- **Inspect:** every permission, its access level, its review state
- **Need:** whether any `pages_*` or `instagram_*` permission is attached
- **Can decide:** the App Review submission plan
- **Cannot decide:** removal of an approved permission without impact analysis

### A-PAGE 7: App Review
- **Inspect:** submissions, outcomes, outstanding requirements
- **Need:** whether Advanced Access is already granted
- **Can decide:** what remains for App A

### A-PAGE 8: Business Settings → portfolio, System Users, assets
- **Inspect:** §17 items 1–11
- **Need:** portfolio ID, System User, WABA ownership, credit line
- **Can decide:** whether App B can use the same portfolio

## APP B — creation order

**Do not start until App A inspection is complete and §22's pre-flight
checklist passes.**

### B-PAGE 1: App creation screen
- **Inspect:** the actual options presented
- **Need:** which type/use case leads to Facebook Login for Business
- **Cannot decide:** in advance — **REQUIRES LIVE META UI VERIFICATION**

### B-PAGE 2: Settings → Basic
- **Record:** App ID (store in Railway, not in chat)
- **Set:** App Domains, privacy policy, terms, icon, category

### B-PAGE 3: Facebook Login for Business → Settings
- **Set:** Valid OAuth Redirect URI — exactly URL #3 (§9)
- **Verify:** HTTPS enforcement, strict mode if present
- **Note:** JS SDK settings are **not** required for App B

### B-PAGE 4: Create the login configuration
- **Set:** standard Facebook Login variation, not Embedded Signup
- **Set:** only the §7 approved permissions
- **Record:** configuration ID → Railway

### B-PAGE 5: Data deletion and deauthorization
- **Set:** URLs #4 and #5 (§9)
- **Verify:** whether Meta requires each

### B-PAGE 6: First live test — Development Mode
- **Run:** the real connect flow end to end
- **Resolve:** `business_management` (§7), `pages_read_engagement` (§7),
  the Instagram field question (Correction 2)
- **This is the screen that resolves the open permission questions.**

### B-PAGE 7: App Review submission
- Only after B-PAGE 6 proves the flow works

---

# SECTION 20 — KNOWN vs UNKNOWN

## KNOWN / ESTABLISHED

**From the VANTA build**
1. Two capability configs exist, credentials never cross
2. Flyer Lady cannot hold a System User token — no such field
3. `GraphApiClient` imports neither config
4. Flyer Lady secret rejected by WhatsApp webhook — real HMAC test
5. WhatsApp secret rejected by Flyer Lady signed request — real HMAC test
6. All six webhook fields the code consumes
7. Every callback path, confirmed in a 101-route URL map
8. Flyer Lady's exact Graph operations: `/me/accounts`, `/photos`, `/photo_stories`
9. Data deletion and deauthorization boundaries, structurally enforced
10. Audit actions identify which app caused each event
11. Legacy fallback is WhatsApp-only
12. Redirect URI validation rejects non-HTTPS, query strings, fragments
13. `integration_status` validates each capability independently
14. 55 tests, 9/9 isolation, zero regressions
15. `billing` does not need Meta credentials

**From the production database**
16. `meta_social_connections`: **0 rows**
17. `meta_business_connections`: **0 rows**

**From Railway**
18. Production domain `app.vantaautomations.co.za` on `phanta-web`
19. Which Meta variable **names** exist on which service
20. `META_FLYER_LADY_CONFIG_ID` absent from both services
21. Past-due subscription warning

## UNKNOWN / REQUIRES VERIFICATION

**Meta account state** — all LIVE UI
22. Business Portfolio name and ID
23. Business Verification status
24. App A's current name, App ID value, mode, type
25. App A's current use cases and products
26. Whether App A holds Page or Instagram permissions
27. App A's configuration IDs (values)
28. WABA existence/ID; phone number/ID/state
29. Current webhook configuration and subscribed fields
30. System User, credit line, payment method
31. Tech Provider onboarding state
32. Current App Review state and Advanced Access
33. Privacy policy, terms, data deletion, deauthorize URLs as configured
34. Any warnings or restrictions

**Meta documentation** — all DOCUMENTATION VERIFICATION
35. Whether `pages_manage_posts` requires `pages_read_engagement`
36. **`business_management` for Flyer Lady — UNRESOLVED**
37. **WhatsApp deauthorization requirement — UNRESOLVED**
38. Whether data deletion is required per app
39. Exact `signed_request` response format expected
40. Current App Review evidence requirements
41. Whether App A needs a Valid OAuth Redirect URI
42. Whether `subscribed_apps` is still required (GAP 1)
43. Whether the signup popup handles phone PIN (GAP 2)
44. Whether ZAR blocks credit sharing
45. Whether `public_profile` must be explicitly submitted

**Does not exist yet**
46. Flyer Lady App ID
47. Flyer Lady App Secret
48. Flyer Lady configuration ID

**Never exercised**
49. **No real Meta API call has ever been made by this codebase in testing.**
50. Behaviour of `instagram_business_account` field without Instagram permissions

---

# SECTION 21 — OPERATOR INPUT CHECKLIST

## Documentation
- [ ] Current official Meta documentation for the WhatsApp use case
- [ ] Current Facebook Login for Business documentation
- [ ] Current Pages API publishing documentation
- [ ] Current App Review requirements
- [ ] Current data deletion and deauthorization requirements

## App A screenshots
- [ ] App Dashboard landing
- [ ] Settings → Basic
- [ ] Facebook Login for Business → Settings
- [ ] Facebook Login for Business → Configurations (list + each detail)
- [ ] WhatsApp → Configuration / API Setup
- [ ] WhatsApp → Webhooks (URL + subscribed fields)
- [ ] Permissions and Features
- [ ] App Review
- [ ] Any warnings or notices

## Business Portfolio screenshots
- [ ] Business info (name, ID, verification status)
- [ ] Apps list
- [ ] WhatsApp Accounts
- [ ] Pages
- [ ] System Users (roles and assigned assets)
- [ ] Payment methods / credit line
- [ ] Any older portfolios and their assets

## Decisions and assets
- [ ] Privacy policy URL
- [ ] Terms URL
- [ ] App icon
- [ ] The Facebook Page for the Flyer Lady demo
- [ ] Confirmation that the operator is portfolio admin

## Railway
- [ ] Variable **names** only — already gathered, re-confirm if changed
- [ ] Confirmation of the subscription/billing state

## ⚠️ NEVER PASTE
- ❌ App Secret (either app)
- ❌ System User token
- ❌ Any access token
- ❌ Authorization code
- ❌ Webhook verify token value
- ❌ `META_TOKEN_ENCRYPTION_KEY`
- ❌ Any `signed_request`

If a screenshot shows a secret, **redact it before sharing.** Meta usually
masks secrets behind a "Show" button — do not click it while capturing.

---

# SECTION 22 — FINAL PRE-FLIGHT CHECKLIST

## BEFORE TOUCHING APP A
- [ ] Code from the separation build is pushed and deployed
- [ ] Deployment verified working on **legacy variables alone**
- [ ] `META_WHATSAPP_*` variables added to `phanta-web` and `phanta-scheduler`
- [ ] `check_env.py` reports "Meta App A — WhatsApp: OK"
- [ ] Current Meta documentation gathered (§21)
- [ ] App A screenshots gathered (§21)
- [ ] Business Portfolio screenshots gathered (§21)
- [ ] Railway subscription resolved
- [ ] `META_TOKEN_ENCRYPTION_KEY` backed up outside Railway
- [ ] Confirmed operator is portfolio admin
- [ ] **Accepted that no real Meta API call has ever been tested**

## BEFORE CREATING APP B
- [ ] App A inspection complete (A-PAGE 1 to 8)
- [ ] Portfolio ID confirmed — App B must go in the same one
- [ ] Confirmed whether App A holds any Page/Instagram permission
- [ ] `pages_read_engagement` dependency resolved (§7)
- [ ] Decided the `business_management` approach (§7)
- [ ] Privacy policy, terms and icon ready
- [ ] Demo Facebook Page identified
- [ ] Business Verification status known

## BEFORE CONFIGURING APP B
- [ ] App B created in the correct portfolio
- [ ] App ID recorded straight into Railway, never into chat
- [ ] App Secret recorded straight into Railway, never into chat
- [ ] Redirect URI #3 registered **exactly**
- [ ] Login configuration created — standard variation, not Embedded Signup
- [ ] Only approved permissions selected
- [ ] Configuration ID recorded into Railway
- [ ] Data deletion URL #4 set
- [ ] Deauthorize URL #5 set
- [ ] `META_FLYER_LADY_*` added to `phanta-web` and `phanta-scheduler`
- [ ] `check_env.py` reports "Meta App B — Flyer Lady: OK"
- [ ] Live connect flow tested in Development Mode (B-PAGE 6)
- [ ] Instagram-field behaviour observed (Correction 2)

## BEFORE APP REVIEW
- [ ] Business Verification complete
- [ ] Both apps have icon, privacy policy, terms, category
- [ ] All permission questions resolved — none submitted "just in case"
- [ ] Every permission has its own video **and** written description
- [ ] **App A and App B videos are entirely separate**
- [ ] App A: GAP 1 (`subscribed_apps`) resolved
- [ ] App A: GAP 2 (phone PIN) resolved
- [ ] App A: message templates created and approved
- [ ] App B: real publishing demonstrated end to end
- [ ] Test instructions and credentials prepared for reviewers

---

# SECTION 23 — FINAL DECISION

## Do we have enough information to safely begin Meta configuration?

# YES — for the inspection phase only.
# NO — for creating or configuring App B.

This is not a hedge. The two phases have genuinely different readiness.

### What is known well enough to start inspecting App A

Every VANTA-side fact an inspection needs to compare against is established
from the actual code, the production database and the live Railway project:
all callback URLs, all webhook fields, the exact permission sets, the
credential boundaries, and the fact that **zero** connections exist on
either capability — so nothing can be orphaned.

The App A inspection (A-PAGE 1 to 8) is **read-only**. It can begin
immediately and is how most of §20's unknowns get resolved.

### What is NOT known well enough to create App B

Four blockers, in order of severity:

**1. `business_management` — genuinely unresolved.**
No code path needs it. It may still be required by Facebook Login for
Business. Cannot be resolved from documentation alone; needs a live test
(B-PAGE 6). Requesting it wrongly risks rejection; omitting it wrongly
breaks authorization.

**2. `pages_read_engagement` — has no code justification.**
Correction 1 above overturns an earlier claim. Either it is a documented
dependency of `pages_manage_posts` — request it and justify it as such — or
it is not, and it must be removed. Submitting it undemonstrated invites
rejection.

**3. WhatsApp deauthorization — unresolved.**
Not implemented. If Meta requires it, publishing App A may be blocked.

**4. Business Portfolio state entirely unknown.**
Creating App B in the wrong portfolio means it cannot access the Pages or
share verification with App A.

### Two further things that are not App-Separation blockers but will block a working product

**GAP 1 — `subscribed_apps` is never called.** Without it, Meta delivers no
webhooks for any onboarded customer, no matter how perfect the
configuration.

**GAP 2 — phone PIN registration has no UI caller.** A workshop may complete
signup, appear connected, and be unable to send a message.

Both are code changes, outside this spec's scope, and must be decided before
a real workshop is onboarded.

### The honest summary

The **code** is ready — 55 tests, 9/9 isolation, zero regressions, verified
to boot on the current production variables. The **Meta side** is not, and
cannot be until screens are opened and documentation is checked.

**Recommended order:**

1. Push and deploy the code — safe now, behaviour unchanged
2. Add `META_WHATSAPP_*` to Railway, verify with `check_env.py`
3. Begin App A inspection, one screen at a time (§19)
4. Resolve the documentation questions in §20
5. Only then create App B
6. Use B-PAGE 6 to settle `business_management` empirically
7. Only then App Review

**What must not happen:** creating App B before the portfolio is confirmed,
or submitting App Review with an unresolved permission. Both are slow and
expensive to undo, and both are avoidable by verifying first.
