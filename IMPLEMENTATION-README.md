# PHANTA — Security & Onboarding Implementation

Everything from the security audit and the onboarding rebuild, applied to the
codebase. This file is the deployment guide; read it before deploying.

## State of the build

- **Tests (SQLite):** 351 passed, 15 skipped, **0 failed**.
- **Tests (PostgreSQL 16):** the 76 behavioural security/onboarding tests all pass
  against a real PostgreSQL database using the restricted `phanta_app` role.
- **RLS verified against real PostgreSQL:** 17/17 (`tests/postgres_rls_verification.py`).
  The two long-standing failures are now fixed. They asserted that no legacy
  `franchise_id`/`tenant_id` scope terms remained in runtime source, and had
  been failing since before this work began. The offending modules turned out
  to be entirely dead — see "Dead code removed" below.
- **Alembic head:** `0030_customer_whatsapp_uniqueness`
- **Migrations added:** 0025 – 0029

---

## Deploy order — this matters

Two steps can lock people out if done in the wrong order.

### 1. Before deploying the code

Run against production, **in this order**:

```
python -m scripts.migrate_plaintext_passwords --dry-run
python -m scripts.migrate_plaintext_passwords          # if the dry run found rows
python -m scripts.inspect_legacy_credential_tables
```

The first converts any account still relying on the removed plaintext-password
path. Deploying the auth change before running it would lock those accounts out.

The second reports whether `whatsapp_numbers`, `messaging_accounts` or
`webhook_events` still hold plaintext Meta tokens. **If it reports credentials,
rotate them at Meta before dropping those tables.** Dropping first destroys the
evidence without addressing the exposure.

### 2. Environment variables

Set in Railway:

| Variable | Value | Notes |
|---|---|---|
| `SENTRY_DSN` | your DSN | **Both** the web and cron services |
| `REGISTRATION_INVITE_CODE` | a code you choose | Without it, registration is disabled in production (fail-closed) |
| `WEB_CONCURRENCY` | `1` | Rate limiting uses in-process memory and is only accurate with one worker |
| `SESSION_LIFETIME_HOURS` | `12` | Sliding inactivity timeout |
| `MESSAGE_BODY_RETENTION_DAYS` | `14` | Must match what your Privacy Policy says |
| `BACKUP_RETENTION_DAYS` | verify | Must match Railway's actual backup window |

**Do not set `RATELIMIT_ENABLED`** in production. It exists only so the test
suite is not throttled.

### 3. Third Railway service

`railway-cron-retention.toml` needs to be created as a **separate service**
(daily, 02:30). You will then have three: web, 5-minute scheduler, retention.
Until it exists, the 14-day message-clearing rule is defined but never runs.

### 4. Migrations

`alembic upgrade head` runs via the existing `preDeployCommand`.

---

## What was built

### Security (13 sessions)

**Defects fixed in shipped features** — things that looked done but did nothing:

- The rate limiter was constructed but never bound to the app. Now bound, with
  limits on login, register, AI reply, messaging, and public booking.
- `log_communication` had 12 columns against 13 placeholders, so every call
  raised. That also silently disabled the 12-hour duplicate-send guard, which
  had therefore never fired.
- `audit_logs` had two disjoint column sets and the only reader saw half the
  rows with a NULL actor.
- A plaintext-password comparison branch was live in authentication.
- `bootstrap.py` assigned `location_id` twice in one UPDATE (PostgreSQL rejects
  this outright) and had a 12-vs-13 mismatch in the INSERT.

**Account security:** server-side session revocation via `session_version`,
session lifetime, change-password route, admin-issued password reset,
`must_reset_password` finally enforced, invite-gated registration, one role
vocabulary with `@require_role` applied to 31 previously ungated endpoints.

**Privacy and compliance:** customer-level marketing consent with evidence,
message categories separating operational from marketing, suppression at the
single send chokepoint, WhatsApp STOP/START handling (including Meta's native
button), legal acceptance records with per-document versioning, tenant data
export, POPIA erasure, a retention engine, a security event log, an incident
register, and two-stage tenant offboarding.

**Infrastructure:** `.dockerignore` (the Dockerfile does `COPY . .`),
`ProxyFix` plus `--forwarded-allow-ips`, and removal of an RLS bypass in the
messaging path.

### Onboarding rebuild

Nine sequential steps:

```
Account → Business → Workshop → WhatsApp → Flyer Lady
        → Automation → Team → Legal → Complete
```

- **Business identity moved to the owner** (`legal_name`,
  `business_registration_number`, `trading_name`, `business_email`). A business
  has one legal identity regardless of how many locations it operates.
- **Shop name = trading name.** The registered name is frequently not the name
  on the door.
- **No VAT in onboarding.** It is billing information, captured at the paywall.
- **Services removed** from onboarding entirely.
- **Operating hours, not time slots.** Mon–Fri as one pair; Saturday and Sunday
  independent and defaulting to closed.
- **Five legal documents, confirmed separately**, each recorded against the
  exact version shown, each opened in a scrollable modal serving the full text.
- **WhatsApp and Flyer Lady are skippable.** Automation requires one *active*
  rule; team is a review screen, not a gate.
- **Completion is derived from the data**, not from a stored percentage, and is
  re-verified server-side.

### Tests

`tests/security/` contains 56 behavioural tests — they drive the app and assert
on status codes and database state, rather than checking that a string appears
in a source file. I verified they work by deliberately breaking five controls
(marketing suppression, export tenant scoping, erasure audit, the VAT rule, the
automation gate); each was caught by exactly the right test.

---

## Still needs a decision from you

1. **Legal retention periods** — seven data types listed in
   `services/retention_service.AWAITING_LEGAL_CONFIRMATION`. Nothing was
   invented; only the 14-day message rule is set.
2. **Legal document versions** — currently `2026-08-28` in
   `services/legal_acceptance_service.REQUIRED_DOCUMENTS`. These must match the
   versions you actually publish, and the placeholders inside
   `legal_documents/*.md` must be filled in.
3. *(resolved)* UI entry points are now linked — see "Reachability" below.
4. **`SOFT_OPT_IN`** — POPIA s69(3) would let workshops market to existing
   customers without a recorded opt-in. The current build requires opt-in, which
   is stricter than the law requires. If you adopt the soft opt-in, **the
   Privacy Policy must be updated first.**
5. **CIPC validation is modern-format only** (`YYYY/NNNNNN/NN`). It will reject
   pre-2002 and CK close-corporation numbers, which some established workshops
   hold. If a customer cannot pass the business step, this is the first thing to
   check — see `validators/cipc_validator.py`.

## PostgreSQL verification (2026-08-28)

The previous gap — RLS never tested against real PostgreSQL — is closed. A
PostgreSQL 16 instance was built, the full migration chain 0001→0029 run, and
the policies exercised as a NON-SUPERUSER role without BYPASSRLS (a superuser
silently bypasses every policy, so testing as one proves nothing).

Verified 17/17: RLS enabled AND forced on customers, bookings, audit_logs,
security_events, security_incidents and legal_acceptances; a tenant cannot read,
fetch by id, insert into, or update another tenant's rows; security_events is
writable with no location context (so failed logins record) but unreadable by a
tenant and readable by a platform admin; security_incidents is unreadable and
unwritable by a tenant; legal acceptances are isolated per tenant; and the
audit_logs location foreign key carries ON DELETE CASCADE.

Re-run it against your own database with:

    DSN=postgresql://user:pass@host:5432/db python3 tests/postgres_rls_verification.py

### Two real bugs this found

**1. A fresh PostgreSQL database could not be bootstrapped at all.**
`_create_tables()` was skipped on PostgreSQL on the assumption that alembic
owned the schema there. It does not: `users` is defined only in
`database/schema.py`, is not an ORM model, and no migration creates it. Predeploy
failed with `relation "users" does not exist`. This was invisible on an existing
production database, which already had the table — it only appeared on a brand-new
one, which is exactly the disaster-recovery path. Fixed in
`database/initialize.py`; every statement is CREATE TABLE IF NOT EXISTS, so it is
a no-op against an existing database.

**2. Tenant offboarding could not write its audit record.**
Offboarding is performed by a platform administrator, but `audit_logs`' platform
policy is SELECT-only, so the INSERT was rejected under PostgreSQL and the whole
offboarding failed. `services/offboarding_service._record()` now writes the audit
row inside the tenant's own RLS scope, which is where it belongs anyway.

Both were pre-existing and neither was detectable on SQLite.

### Test fixtures now run on both backends

`tests/rls_helpers.py` provides `platform_scope()` and `location_scope()` so the
behavioural suites seed data through the same RLS mechanisms production uses.
Previously those suites could only run on SQLite — the backend that ignores the
isolation they exist to prove.


---

## Dead code removed

Six modules were deleted, totalling ~640 lines. All were entirely unreferenced,
and three of them would have raised at runtime if anything had called them:

| Module | Why it went |
|---|---|
| `helpers/tenant.py` | `current_tenant_id()`, reading a `franchise_id` session key that no longer exists |
| `services/franchise_service.py` | franchise-era, no importers |
| `repositories/franchise_repository.py` | imported only by the above |
| `repositories/tenant_guard.py` | `TenantGuard` referenced `Customer.tenant_id` — a column that does not exist |
| `integrations/meta/webhook/webhook_tenant_resolver.py` | referenced `MetaBusinessConnection.tenant_id`, which does not exist; the live path uses `_resolve_meta_webhook_location` |
| `integrations/paystack/webhooks/webhook_tenant_resolver.py` | superseded twin of `webhook_location_resolver`, which is what actually runs |

Two existing tests had been guarding against exactly this and failing quietly.
The suite is now fully green on both backends, so those guards are live again.

## Reachability

Routes that exist but nothing links to are not features. Three were unreachable:

- **`/settings/` was a redirect** straight to `/settings/business`, so no other
  settings page could be found from the interface — including the
  change-password and data-export pages built during the security work. It is
  now a real hub, listing only the sections the signed-in user may open.
- **The POPIA erasure route** had no button. There is now one on the customer
  profile, owner/admin only, with a confirmation dialog and wording that
  explains what survives erasure.
- **The platform security-events page** is now linked from the platform
  dashboard.

Two tests in `tests/security/test_onboarding_behaviours.py` assert these links
exist, so they cannot silently disappear again.


---

## "Faulty connections" — what they actually were

Every integration endpoint that appeared broken was in fact **unconfigured**.
The configuration loaders raise on missing credentials:

    RuntimeError: Missing Meta authentication configuration: app_id, app_secret,
                  system_user_token
    RuntimeError: META_WEBHOOK_VERIFY_TOKEN is required

Nothing caught those, so seven endpoints returned unhandled **500**s. A 500 says
"PHANTA failed"; the truth was "this deployment has not been given its
credentials". Those are different problems with different fixes, and the error
gave no way to tell them apart.

Every GET route was smoke-tested as a signed-in owner. Results:

| Before | After |
|---|---|
| 7 unhandled 500s / exceptions | **0** |
| — | 7 explanatory **503**s naming the missing variables |

The affected endpoints: Meta embedded-signup config, connection health, phone
info, WABA info, Flyer Lady connect, Google Business connect, and the Meta
webhook verification handshake. 503 is the honest code — the feature works, it
just is not set up here. Responses name variables only, never values.

### Also fixed: a template that built a route name by string concatenation

`templates/onboarding.html` did `url_for('onboarding.onboarding_' + current_step)`,
with `current_step` derived from a hardcoded percentage ladder that still
included the removed `services` step. Any account whose progress landed on that
step got a **BuildError — a 500 on the onboarding page**. The next stage is now
resolved to a real registered endpoint in Python, so the template can never be
asked to build a route that does not exist.

### New: integration status page

`/platform/dashboard/integration-status` (platform admin, also `?format=json`)
shows which integrations have their credentials on this deployment. Linked from
the platform dashboard. It reports variable NAMES only — never values — so it is
safe to screenshot when asking for help.

### The 404s were not faults

Six routes returned 404 during the sweep: `/vehicles/1`, `/book/<slug>` and
similar. All were missing seed data, not broken wiring — they return 200 once
the records exist. Verified.

### Locked in by tests

`tests/security/test_onboarding_behaviours.py` now asserts that unconfigured
integrations return 503 rather than 500, that credential values never appear in
the status output, and — importantly — that **every `url_for()` target in every
template and route resolves to a registered endpoint**. That last test would
have caught the onboarding BuildError before it shipped.


---

## Franchise-era SQL defects (systematic sweep)

The franchise -> owner/location migration left a recurring defect: a scope value
passed twice, so a statement supplied one more value than it had columns. Every
affected statement raised at runtime. A static sweep of all hand-written SQL
found **eight**, all now fixed:

| File | Defect |
|---|---|
| `services/catalog_service.py` | INSERT 6 columns / 7 placeholders, and the SELECT above it had 3 placeholders for 2 parameters |
| `services/vehicle_service.py` | 20 columns / 21 placeholders |
| `services/reminder_service.py` | 12 columns / 13 values, in two places |
| `repositories/booking_repository.py` | 39 columns / 41 placeholders |
| `repositories/lead_repository.py` | 8 columns / 9 placeholders in both branches, plus a duplicated `location_id` |
| `services/booking_service.py` | duplicate `"location_id"` dict key silently discarding a value; `ensure_service()` called with three arguments when it takes two |
| `database/schema.py` | `"services"` appeared **twice** in the column-ensure dict. Python keeps only the last, so `description` and `display_order` were never ensured. Merged. |
| `services/booking_mapper.py` | module-level code referencing undefined names — raised `NameError` on import. Deleted (never imported). |

### Where these actually sat

Most were in code paths that are **no longer reached**: the module-level
`insert_booking` in `services/booking_service.py` has no callers (the live path
is the `BookingService` class), and the legacy reminder generator is not reached
from the cron scheduler (which uses `ai/communications/lifecycle.py`). They were
real bugs but dormant — which is exactly why they survived: nothing exercised
them and nothing checked them.

Worth knowing for maintenance: **there are two booking implementations and two
reminder implementations in this codebase, and the dead ones are the broken
ones.**

### Guarded permanently

`tests/security/test_sql_statement_integrity.py` adds three static checks:

- INSERT column count vs value count (bracket-balanced, so `NOW()` and
  `COALESCE(...)` in a VALUES list do not confuse it)
- UPDATE assigning the same column twice — PostgreSQL rejects this outright
- duplicate keys in dict literals, which Python silently collapses

Mutation-tested by reintroducing the catalog defect; the check caught it.

## Other checks run clean

- **ORM vs database schema:** every model column exists, on both SQLite and
  PostgreSQL. 0 mismatches.
- **Unreachable code / bare `except:`:** none in production code.
- **Swallowed exceptions:** 11 `except: pass`, each reviewed — all narrow
  exception types falling back to defaults on malformed stored JSON. Correct.
- **Duplicate `WHERE` comparisons:** 5 candidates, all false positives
  (separate statements).

## Two small fixes

- `integrations/paystack/webhooks/webhook_handler.py` imported the dispute
  handler as a bare `handle`, the same name as the class's own `handle` method.
  It resolves correctly — verified by executing a signed dispute webhook
  end-to-end — but reads as though the method calls itself, and any future
  module-level `handle` would silently change which function runs. Renamed to
  `handle_dispute_created`.
- `services/financial_service.py` had its docstring two statements into the
  function, making it a no-op string rather than documentation. Repositioned.


---

## CORRECTION: marketing consent was not on the live send path

This corrects a claim made when the consent work was first delivered.

Marketing suppression was implemented in
`services/messaging_service.can_send_outbound()`, described at the time as "the
single chokepoint every send path passes through". It is not. That function is
reached only from `services/reminder_service.py` and
`services/inquiry_followup_service.py`, and **neither module is imported by
anything**. Confirmed empirically: after exercising every GET route and running
the scheduler, neither appears in `sys.modules`.

The live outbound path is `ai/communications/lifecycle.LifecycleCommunicationService`
and `MetaMessagingService`, used by `routes/lifecycle.py`, `routes/bookings.py`,
`routes/public_booking.py` and `routes/webhooks.py`. It contained no consent
check of any kind.

So a customer's WhatsApp STOP was recorded correctly and then never consulted
when sending.

**Why the tests did not catch it:** they called `can_send_outbound()` directly.
The behaviour was correct; the reachability was not. Unit tests cannot detect
that the function under test is unreachable.

**Practical exposure was limited** -- the live lifecycle sends only operational
messages (booking confirmed, reminder, ready for collection, missed booking,
yearly service), which should send regardless of a marketing opt-out. Nothing
was sent in violation of an opt-out. But the protection would not have held the
moment anything promotional was sent through the live path.

### The fix

The gate now lives in `LifecycleCommunicationService._send()`, the single funnel
every lifecycle message passes through, using the same category rules:
operational categories always send; `MARKETING_CATEGORIES` require affirmative
consent; unknown consent state suppresses. All five live call sites now declare
an explicit category.

### Guarded by reachability tests, not just behaviour tests

Three new tests in `tests/security/test_security_behaviours.py`:

- the live `_send()` must consult consent
- every live `_send()` call site must declare a category
- an opted-out customer is suppressed for marketing and still receives
  operational messages

Mutation-tested: bypassing the gate and removing a category label each caused
the right test to fail.

### Still outstanding

`services/booking_service.py`, `services/reminder_service.py` and
`services/inquiry_followup_service.py` remain in the tree, unreferenced. They
are now genuinely redundant rather than load-bearing, but deleting them is a
larger change than this pass and has not been done.


---

## Reachability audit — the general form of the consent bug

The marketing-consent defect above had a general shape worth checking for: a
control that is correctly implemented, correctly unit-tested, and never
executed. Every security control was therefore audited for reachability by
booting the app, exercising every GET route, running the scheduler, and asking
which modules were actually imported.

Result: all controls reachable **except two**.

`services/incident_service.py` and `services/offboarding_service.py` had no
route, no script, and no importer anywhere. They could only be driven from a
Python shell. For the incident register that is the worst possible property --
during a real incident, the moment it matters, there would have been no way to
use it without writing code under pressure.

### Entry points added

    python -m scripts.incident open --type ... --severity ... --summary ...
    python -m scripts.incident list | show <id> | update <id> | scope <id>

    python -m scripts.offboard status --location-id N
    python -m scripts.offboard stop   --location-id N --actor you
    python -m scripts.offboard delete --location-id N --actor you

CLI rather than admin pages, deliberately:

  * an incident may involve the web application being unavailable or
    untrusted, so the record should not live only inside it;
  * stage 2 of offboarding permanently anonymises every customer of a
    workshop, which should not sit one mis-click away in a web interface.

`offboard delete` refuses to run without a recorded data export (`--force`
records that the workshop declined it) and requires typing the location id to
confirm. Verified: the guard holds and customer data is left untouched.

### Guarded

`tests/security/test_security_behaviours.py` now asserts that every security
control has an importer somewhere in the running application, and that both
CLIs exist with their safety features intact. Mutation-tested by deleting
`scripts/incident.py` -- two tests failed immediately.

This check is the one that would have caught the consent bug months earlier.


---

## The upgrade path — migrations silently did nothing on a real database

Everything above was verified against a **fresh** PostgreSQL database. Your
actual deployment is an **upgrade**: applying 0025-0029 to a database that
already holds data. That path was tested separately, and it was broken.

### What happened

A database was built, stamped at 0024 to simulate production, seeded with two
workshops and deliberately awkward rows -- an audit record with a NULL location,
and one pointing at a location id that does not exist. Then the real deploy
command was run.

It reported success. It had done almost nothing:

- the orphaned audit row **survived**
- `audit_logs.location_id` was **not** backfilled
- and PostgreSQL reported the new foreign key as `convalidated = true`

The database then believed a constraint held that it did not.

### Why

Migrations run as the application role, which is subject to
`FORCE ROW LEVEL SECURITY`, and no `app.location_id` is set during a migration.
The role therefore sees **no rows at all**. Every `UPDATE` affected zero rows
and raised no error. The foreign key's validation scan also saw zero rows, so
it "passed".

Setting `app.platform_admin` does not help: the platform policies grant SELECT
only, so UPDATE stays blocked.

### The fix

Migrations 0026 and 0029 now `DISABLE ROW LEVEL SECURITY` on the specific table
they repair, do the data work, then restore `ENABLE` + `FORCE` immediately.
They run as the table owner, so this is permitted. 0026 additionally re-counts
violating rows before adding the constraint and raises rather than creating a
foreign key that would be validated against invisible data.

Verified on a seeded upgrade: orphan cleared, `violating = 0`, all three audit
rows preserved, and RLS confirmed back to `rls = t, forced = t` on both
`audit_logs` and `legal_acceptances`.

One row is deliberately left with a NULL location: its acting user is the
platform superadmin, who has no location. The migration backfills only where a
location is genuinely derivable.

### Guarded

`tests/security/test_sql_statement_integrity.py` now fails any migration that
changes data in an RLS-protected table without disabling row level security, or
that disables it without restoring `ENABLE` + `FORCE`. Mutation-tested.

### What this means for your deploy

Re-run `alembic upgrade head` after applying this build. On a database where
0026 already ran under the old code, the foreign key exists but was never
validated against real data, and the backfill never happened. The corrected
0026 will not re-run automatically -- if you have already deployed the previous
build to production, check for orphaned rows manually:

    SELECT COUNT(*) FROM audit_logs
    WHERE location_id IS NOT NULL
      AND location_id NOT IN (SELECT id FROM locations);


---

## ADMIN_DATABASE_URL — required in production, was undocumented

`database/migrations.py` refuses to run Alembic without `ADMIN_DATABASE_URL`
whenever the environment indicates production. Without it, `preDeployCommand`
exits 1 and **the deploy fails before the app starts**. It was not in
`.env.example` or any checklist. Now documented.

Set it to the same value as `DATABASE_URL` unless you want migrations to use a
more privileged role.

## 0030 — one customer per WhatsApp number per workshop

`customers` had only a non-unique index on (location_id, phone, email). Nothing
stopped two rows for the same WhatsApp number in the same workshop: two inbound
messages from the same person arriving simultaneously could each create a
customer, splitting their history across two records.

Added as a **partial** unique index on (location_id, whatsapp_number),
excluding NULL numbers and soft-deleted rows. Verified on PostgreSQL:

| Case | Result |
|---|---|
| Duplicate number, same workshop | rejected |
| Same number, different workshop | allowed (each workshop owns its record) |
| Two customers with no WhatsApp number | allowed |
| New customer reusing an erased customer's number | allowed |

**On existing duplicates the migration refuses rather than merging.** Merging
customer records is a business decision -- which name wins, which vehicles and
bookings move -- and guessing risks attributing one person's service history to
another. Verified against a database seeded with duplicates: predeploy exits 1
in production, names the offending records, leaves the Alembic version at 0029,
does not create the index, and **leaves row level security enabled**.


---

## Environment check

Most deployment failures here are a missing environment variable, and the
symptom rarely names it. Run this on each service before deploying:

    python -m scripts.check_env          # web service
    python -m scripts.check_env --cron   # cron services

Exit 0 = safe to deploy. Exit 1 = something required is missing, with an
explanation of what each missing variable actually breaks.

On a bare production deployment it reports **six** blocking variables:
`DATABASE_URL`, `FLASK_SECRET_KEY`, `SUPERADMIN_PASSWORD`,
`META_TOKEN_ENCRYPTION_KEY`, `ADMIN_DATABASE_URL`, `REGISTRATION_INVITE_CODE`.

It also flags values that are set but wrong -- `WEB_CONCURRENCY` above 1
silently multiplies every rate limit, and `RATELIMIT_ENABLED=false` disables
rate limiting entirely.

Values are never printed, only whether they are set, so it is safe to run in a
shared terminal and safe to screenshot when asking for help. There is a test
asserting that.
