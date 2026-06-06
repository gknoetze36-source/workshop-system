# Security Audit

Audit scope:

- `app.py`
- `database.py`
- `platform_helpers.py`
- `platform_messaging.py`
- `services/paystack.py`
- templates
- migrations
- deployment files

## Authentication

UI login:

- route `GET,POST /login`
- function `login()`
- password verification with `werkzeug.security.check_password_hash()`
- legacy plaintext password support is migrated on successful login
- session key: `session["user_id"]`

API login:

- route `POST /api/auth/login`
- function `api_auth_login()`
- returns signed token from `_issue_api_token()`
- token validated by `_user_from_api_token()`

Password change:

- route `GET,POST /account/password`
- enforces current password
- requires new password length >= 10

## Authorization and RBAC

Decorators:

- `login_required()`
- `roles_required(*roles)`

Roles:

- `super_admin`
- `franchise_admin`
- `reception`

Tenant scoping:

- `platform_helpers.scope_clause()`
- `platform_helpers.user_scope_clause()`
- `platform_helpers.assert_tenant_scope()`
- `platform_helpers.selected_branch_for_user()`

## Sessions

Session config in `app.py`:

- `SESSION_COOKIE_SECURE`, default true
- `SESSION_COOKIE_HTTPONLY=True`
- `SESSION_COOKIE_SAMESITE`, default `Lax`
- `PERMANENT_SESSION_LIFETIME`, default 12 hours

Secret:

- `SECRET_KEY` is boot-critical.

## CSRF

CSRF is enabled through:

- `csrf = CSRFProtect(app)`

Forms receive CSRF token through JavaScript injection in `templates/base.html`.

CSRF exemptions:

- `/api/<path:_path>` OPTIONS
- `POST /api/auth/login`
- many API routes decorated with `@csrf.exempt`
- `POST /webhook/paystack`
- `GET,POST /webhooks/meta/...`
- public booking webhook

Webhook routes must remain CSRF exempt because external systems call them.

## Secrets and Tokens

Messaging token:

- `messaging_accounts.access_token`
- encrypted by `encrypt_access_token()`
- decrypted by `decrypt_access_token()`
- requires `MESSAGING_TOKEN_ENCRYPTION_KEY`

Paystack:

- `PAYSTACK_SECRET_KEY`
- `PAYSTACK_WEBHOOK_SECRET`

Meta:

- `META_APP_ID`
- `META_APP_SECRET`
- `META_ACCESS_TOKEN`
- `VERIFY_TOKEN`
- `META_EMBEDDED_SIGNUP_REDIRECT_URI`

## Webhook Validation

Meta:

- route token validates `franchises.inbound_webhook_token`
- signature validates `X-Hub-Signature-256`
- replay protected by `webhook_events`

Paystack:

- signature validates `x-paystack-signature`
- transaction is verified by API call
- replay protected by `paystack_webhook_events`

## SQL Injection

Most database calls use parameterized `%s` arguments via `query_db()` and `execute_db()`.

Risk areas:

- dynamic SQL in helper functions where clauses are composed from controlled values.
- direct f-string DDL in `database._ensure_columns()` is internal schema maintenance only.

## XSS

Templates use Jinja autoescaping by default. Stored message bodies and notes are rendered through templates; avoid adding `|safe` unless reviewed.

## Findings

### CRITICAL

No unresolved critical finding confirmed in current code scan.

### HIGH

1. `reset_live_passwords.py` contains hardcoded `TEMP_PASSWORD = "password1234"`.
   - Risk: accidental production use resets known accounts to weak password.
   - Action: remove from production or require env-provided password and confirmation.

2. `reset_all_passwords()` uses shared `login1234`.
   - File: `app.py`
   - Route: `POST /manage/credentials/reset-all`
   - Existing mitigation: `must_reset_password` is set.
   - Action: generate unique per-user temporary passwords.

3. `ALLOW_PUBLIC_DASHBOARD_API` grants superadmin-style API context.
   - File: `app.py`
   - Function: `_frontend_api_authorized()`
   - Action: never enable in production.

4. `FRONTEND_API_TOKEN` grants broad API access.
   - File: `app.py`
   - Function: `_frontend_api_authorized()`
   - Action: rotate regularly and prefer user bearer tokens.

### MEDIUM

1. Demo accounts are bootstrapped by `_ensure_demo_access_accounts()`.
   - File: `database.py`
   - Action: gate with explicit production flag or disable in production.

2. `whatsapp_numbers.access_token` remains in legacy table.
   - Risk: token storage path outside current encryption design.
   - Action: migrate/disable legacy table usage.

3. `webhook_events` and `paystack_webhook_events` have no retention.
   - Risk: unbounded growth.
   - Action: add archival/retention job.

4. Meta token refresh is not automated.
   - Risk: token expiry stops messaging.
   - Action: operational token rotation calendar.

5. Some foreign keys are `NOT VALID`.
   - File: `20260519_0001_production_constraints.py`
   - Action: validate after data cleanup.

### LOW

1. Empty `routes/*.py` placeholder Blueprints may confuse maintainers.
2. `sentry-sdk` is installed but not configured.
3. `billing@example.com` fallback exists for Paystack link email.
4. `app.py` is a large monolith.

## Tenant Isolation Score

Tenant isolation is implemented but mixed between app logic and DB constraints.

Score: 8/10.

Strengths:

- `scope_clause()` and `selected_branch_for_user()` used in core workflows.
- Meta account uniqueness constraints.
- Webhook phone ID routing.

Weaknesses:

- Runtime schema still relies heavily on application-level scoping.
- Some admin/audit views intentionally cross tenants for superadmin.
- Not all tables have strict validated FKs.

## Security Score

SECURITY_SCORE: 76/100.

NO-GO items before broad unattended production:

- remove or guard known-password reset paths
- disable public dashboard API
- confirm production demo account policy
