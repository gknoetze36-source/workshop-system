# VANTA Owner Playbook

This playbook documents the repository as implemented. It is based on the files in this repository, especially `app.py`, `database.py`, `platform_helpers.py`, `platform_messaging.py`, `automation_engine.py`, `services/paystack.py`, `scheduler.py`, `cron_jobs.py`, `railway.json`, `Procfile`, `Dockerfile`, `requirements.txt`, `package.json`, `prisma/schema.prisma`, and the migrations in `database/migrations/versions/`.

## Ownership Summary

VANTA is a Flask-based SaaS workshop management platform. Runtime tenant isolation is implemented as:

```text
SUPERADMIN -> Franchise -> Branch -> Users
```

The runtime application is franchise-centric. `workshops` exists as a SaaS root table and `franchises.workshop_id` maps runtime franchises to SaaS workshop IDs, but most route logic scopes data using `franchise_id` and `branch_id`.

Primary runtime entry point:

```text
app.py -> Flask app -> initialize_database() -> routes/templates/workers
```

Primary deployment services:

```text
web       -> gunicorn app:app
worker    -> python automation_worker.py
scheduler -> python scheduler.py
billing   -> python cron_jobs.py billing
```

## Repository Inventory

| File | Purpose | Dependencies | Business Function | Criticality |
|---|---|---|---|---|
| `.gitignore` | Git ignore rules | Git | Prevents local/generated files from entering repo | MEDIUM |
| `app.py` | Main Flask application, routes, auth, webhooks, admin UI | Flask, database.py, platform_helpers.py, platform_messaging.py, services/paystack.py | Core SaaS runtime | CRITICAL |
| `database.py` | Runtime schema bootstrap, connection pooling, migrations, seeds | psycopg2, sqlite3, Alembic | Database authority for running app | CRITICAL |
| `platform_helpers.py` | Tenant scoping, RBAC helpers, billing helpers, booking creation | database.py, validators | Business logic and tenant isolation | CRITICAL |
| `platform_messaging.py` | Meta provider, encrypted tokens, reminders, followups, messaging logs | requests, cryptography, database.py | WhatsApp and reminder messaging | CRITICAL |
| `automation_engine.py` | Scheduled job queue processor | database.py, platform_messaging.py | Automation execution | CRITICAL |
| `automation_worker.py` | Long-running automation worker | automation_engine.py, database.py | Background queue processing | HIGH |
| `scheduler.py` | Long-running reminder scheduler | cron_jobs.py, database.py | Daily reminders/followups | HIGH |
| `cron_jobs.py` | One-off job runner for reminders, billing, subscriptions | platform_messaging.py, platform_helpers.py | Railway cron/ops jobs | HIGH |
| `services/paystack.py` | Paystack API, signatures, webhook idempotency | requests, database.py | Billing payments | CRITICAL |
| `assistant_engine.py` | Simple stateful booking assistant | database.py, platform_helpers.py | Inbound assistant replies | MEDIUM |
| `ai_engine.py` | OpenAI message classification helper | openai env var | Optional AI classification | MEDIUM |
| `deployment_check.py` | Local env validation | database.py | Deploy readiness check | HIGH |
| `setup_db.py` | Manual DB bootstrap runner | database.py | Local/admin DB setup | MEDIUM |
| `reset_live_passwords.py` | Resets known users to a fixed temp password | database.py, werkzeug | Emergency reset script; dangerous in production | HIGH |
| `requirements.txt` | Python dependencies | pip | Runtime package install | CRITICAL |
| `package.json` | Prisma scripts and JS dependencies | npm, Prisma | Prisma validation/generation | MEDIUM |
| `package-lock.json` | Locked npm dependency tree | npm | Reproducible JS deps | MEDIUM |
| `railway.json` | Railway web start command | Railway, gunicorn | Web deployment | CRITICAL |
| `Procfile` | web/worker/scheduler/billing process definitions | Railway/Procfile runner | Multi-service ops | HIGH |
| `Dockerfile` | Container build | Python 3.11 slim, requirements.txt | Container deployment | HIGH |
| `alembic.ini` | Alembic config | Alembic | DB migrations | HIGH |
| `prisma.config.ts` | Prisma config | Prisma | Prisma tooling | LOW |
| `prisma/schema.prisma` | Prisma schema for vanta_core model | Prisma | Reference/ORM schema | MEDIUM |
| `bookings.csv` | Legacy/import seed data | database.py import path | Bootstrap booking import | LOW |
| `RAILWAY_DEPLOYMENT.md` | Railway deployment guide | Docs | Ops documentation | HIGH |
| `DEPLOYMENT_CHECKLIST.md` | Deployment checklist | Docs | Ops checklist | MEDIUM |
| `NEW_CLIENT_SETUP.md` | Existing client setup doc | Docs | Onboarding doc | MEDIUM |
| `PAYSTACK_SETUP.md` | Existing Paystack doc | Docs | Billing setup doc | MEDIUM |
| `PLATFORM_UPGRADE.md` | Existing upgrade notes | Docs | Upgrade planning | LOW |
| `user_creation_setup.md` | User creation procedure | Docs | Admin onboarding | MEDIUM |
| `database/connection.py` | Railway DB helper settings | os env | Pool settings reference | MEDIUM |
| `database/schema/vanta_core.sql` | SQL schema for core vanta_core | PostgreSQL | Reference schema | MEDIUM |
| `database/schema/seed.sql` | SQL seed data | PostgreSQL | Seed data | LOW |
| `database/schema/.gitkeep` | Keeps folder tracked | Git | None | LOW |
| `database/bootstrap/.gitkeep` | Keeps folder tracked | Git | None | LOW |
| `database/seeders/.gitkeep` | Keeps folder tracked | Git | None | LOW |
| `database/migrations/env.py` | Alembic runtime env | Alembic, DATABASE_URL | Migration execution | HIGH |
| `database/migrations/script.py.mako` | Alembic revision template | Alembic | Migration generation | LOW |
| `database/migrations/versions/20260519_0001_production_constraints.py` | Initial production indexes/FKs | Alembic | DB hardening | HIGH |
| `database/migrations/versions/20260601_0002_meta_provider.py` | Meta provider columns | Alembic | Meta readiness | HIGH |
| `database/migrations/versions/20260601_0003_messaging_security.py` | Webhook replay and Meta uniqueness | Alembic | Messaging security | CRITICAL |
| `database/migrations/versions/20260605_0004_audit_paystack_admin.py` | Audit and Paystack idempotency | Alembic | Audit/billing hardening | HIGH |
| `routes/__init__.py` | Empty route package | Flask package | Placeholder only | LOW |
| `routes/admin.py` | Empty Blueprint `admin` | Flask | Placeholder only | LOW |
| `routes/auth.py` | Empty Blueprint `auth` | Flask | Placeholder only | LOW |
| `routes/billing.py` | Empty Blueprint `billing` | Flask | Placeholder only | LOW |
| `routes/bookings.py` | Empty Blueprint `bookings` | Flask | Placeholder only | LOW |
| `routes/chatbot.py` | Empty Blueprint `chatbot` | Flask | Placeholder only | LOW |
| `routes/reminders.py` | Empty Blueprint `reminders` | Flask | Placeholder only | LOW |
| `routes/webhooks.py` | Empty Blueprint `webhooks` | Flask | Placeholder only | LOW |
| `validators/phone_validator.py` | SA phone normalization/validation | re | Booking/customer phone validation | HIGH |
| `validators/request_validator.py` | Required field and integer validation | Flask abort | Request validation | MEDIUM |
| `validators/__init__.py` | Package marker | Python | None | LOW |
| `services/__init__.py` | Package marker | Python | None | LOW |
| `workers/__init__.py` | Package marker | Python | None | LOW |
| `tests/test_meta_messaging.py` | Meta provider/webhook/token tests | unittest, mocks | Regression tests | HIGH |
| `templates/base.html` | Shared HTML shell/nav/CSRF injection | Flask/Jinja | UI base | CRITICAL |
| `templates/login.html` | Login page | Flask/Jinja | Auth UI | HIGH |
| `templates/dashboard.html` | Main dashboard | Flask/Jinja | Operations UI | HIGH |
| `templates/bookings.html` | Booking list | Flask/Jinja | Booking management | HIGH |
| `templates/booking_detail.html` | Booking detail/update UI | Flask/Jinja | Booking workflow | HIGH |
| `templates/booking_form.html` | Reception/walk-in booking form | Flask/Jinja | Booking creation | HIGH |
| `templates/public_home.html` | Public booking landing | Flask/Jinja | Public intake | MEDIUM |
| `templates/public_booking.html` | Public booking form | Flask/Jinja | Public intake | HIGH |
| `templates/booking_success.html` | Public success page | Flask/Jinja | Booking confirmation UI | MEDIUM |
| `templates/customers.html` | Customer list | Flask/Jinja | Customer management | MEDIUM |
| `templates/customer_history.html` | Customer history view | Flask/Jinja | Customer support | MEDIUM |
| `templates/reports.html` | Reports page | Flask/Jinja | Reporting | MEDIUM |
| `templates/reminders.html` | Reminder management | Flask/Jinja | Reminder operations | HIGH |
| `templates/manage_franchises.html` | Franchise/billing admin | Flask/Jinja | Superadmin SaaS admin | CRITICAL |
| `templates/manage_branches.html` | Branch/location admin | Flask/Jinja | Branch management | HIGH |
| `templates/manage_users.html` | User admin | Flask/Jinja | RBAC/user management | CRITICAL |
| `templates/manage_prices.html` | Service pricing admin | Flask/Jinja | Pricing setup | MEDIUM |
| `templates/manage_credentials.html` | Credential reset/audit UI | Flask/Jinja | Credential operations | HIGH |
| `templates/chatbot_inbox.html` | Chatbot inbox admin | Flask/Jinja | Inbound message handling | MEDIUM |
| `templates/admin_organization.html` | Superadmin organization view | Flask/Jinja | Organization audit | HIGH |
| `templates/admin_client_audit.html` | Integration/audit center | Flask/Jinja | Meta/Paystack/audit ops | CRITICAL |
| `templates/meta_signup_select.html` | Meta asset selection page | Flask/Jinja | Embedded Signup recovery | HIGH |
| `templates/password_reset.html` | Password change page | Flask/Jinja | Account security | HIGH |
| `templates/error.html` | Error page | Flask/Jinja | UX/error handling | LOW |

## Frameworks and Dependencies

Python dependencies in `requirements.txt`:

- `Flask>=3.0,<4.0`: web framework.
- `Flask-WTF>=1.2,<2.0`: CSRF handling.
- `Flask-Limiter>=3.8,<4.0`: rate limiting.
- `gunicorn>=23.0,<24.0`: production WSGI server.
- `psycopg2-binary>=2.9,<3.0`: PostgreSQL driver.
- `python-dotenv>=1.0,<2.0`: environment loading support.
- `SQLAlchemy>=2.0,<3.0`: used by Alembic migration environment.
- `alembic>=1.13,<2.0`: migration runner.
- `requests>=2.32,<3.0`: Meta and Paystack HTTP calls.
- `sentry-sdk[flask]>=2.0,<3.0`: installed but not wired in code.
- `openai>=1.0,<2.0`: optional AI helper in `ai_engine.py`.
- `cryptography>=42.0,<45.0`: Fernet token encryption in `platform_messaging.py`.

Node dependencies in `package.json`:

- `@prisma/client`
- `prisma`

## Third-Party Services

- Railway: hosting, web/worker/scheduler/billing services, PostgreSQL.
- PostgreSQL: production database.
- Meta WhatsApp Cloud API: outbound/inbound WhatsApp.
- Paystack: billing payment links and payment webhooks.
- OpenAI: optional message classification through `ai_engine.py`.

## Internal Services

- Flask web service: `app.py`.
- Database bootstrap/migration service: `database.initialize_database()`.
- Automation worker: `automation_worker.run_worker()`.
- Scheduler: `scheduler.run_scheduler()`.
- Cron runner: `cron_jobs.py`.
- Paystack service: `services/paystack.py`.
- Meta provider: `platform_messaging.MetaCloudApiProvider`.

## Startup Sequence

1. Railway starts web command from `railway.json`:

```bash
gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers ${WEB_CONCURRENCY:-1} --threads ${GUNICORN_THREADS:-2} --timeout ${GUNICORN_TIMEOUT:-60}
```

2. `app.py` imports helpers and creates `app = Flask(__name__)`.
3. `validate_startup_environment()` checks `DATABASE_URL` or Railway `PG*` fallback plus `SECRET_KEY`.
4. Flask config sets secure cookies and session lifetime.
5. Logging is configured through `JsonFormatter`.
6. `initialize_database()` runs unless `SKIP_DATABASE_INIT=true`.
7. `initialize_database()` creates tables, ensures columns/indexes, runs Alembic migrations, seeds plans/templates, ensures superadmin/demo access.
8. Requests run through `load_current_user()` except `/health`.

## Core Routes

Health:

- `GET /health` -> `health()`
- `GET /health/db` -> `health_db()`
- `GET /admin/system-status` -> `system_status()`

Auth/API:

- `POST /api/auth/login` -> `api_auth_login()`
- `GET /api/me` -> `api_me()`
- `GET /api/dashboard` -> `api_dashboard()`
- `GET /api/jobs` -> `api_jobs()`
- `GET,POST /api/bookings` -> `api_bookings()`
- `GET /api/customers` -> `api_customers()`
- `GET /api/vehicles` -> `api_vehicles()`
- `GET /api/automations` -> `api_automations()`
- `GET /api/staff` -> `api_staff()`
- `GET /api/inventory` -> `api_inventory()`
- `GET /api/reports` -> `api_reports()`
- `GET /api/billing` -> `api_billing()`
- `GET /api/settings` -> `api_settings()`

Public:

- `GET /` -> `home()`
- `GET,POST /book` -> `public_booking()`
- `GET,POST /book/<franchise_slug>/<branch_slug>` -> `public_branch_booking()`
- `POST /webhook/booking/<franchise_slug>/<branch_slug>/<token>` -> `booking_webhook()`
- `GET /booking-success/<reference>` -> `booking_success()`

User:

- `GET,POST /login` -> `login()`
- `GET,POST /account/password` -> `change_password()`
- `GET /signup` -> `signup_redirect()`
- `GET /logout` -> `logout()`

Operations:

- `GET /dashboard` -> `dashboard()`
- `GET /bookings` -> `bookings()`
- `GET /bookings/<reference>` -> `booking_detail()`
- `POST /bookings/<reference>/quick-update` -> `quick_update_booking()`
- `POST /bookings/<reference>/update` -> `update_booking()`
- `GET,POST /add` -> `add_booking()`
- `GET,POST /walkin` -> `walkin()`
- `GET /customers` -> `customers()`
- `GET /customers/history` -> `customer_history_query()`
- `GET /customers/<path:phone>` -> `customer_history()`
- `GET /reports` -> `reports()`
- `GET /reminders` -> `reminders()`
- `POST /reminders/run` -> `run_reminders()`
- `POST /reminders/<int:reminder_id>/send/<channel>` -> `send_reminder()`

Superadmin/admin:

- `GET,POST /manage/franchises` -> `manage_franchises()`
- `POST /manage/franchises/<int:franchise_id>/update` -> `update_franchise()`
- `POST /manage/franchises/<int:franchise_id>/provision` -> `provision_franchise()`
- `POST /admin/failed-jobs/<int:failed_job_id>/retry` -> `retry_failed_automation_job()`
- `GET /admin/organization` -> `admin_organization()`
- `GET /admin/client-audit` -> `admin_client_audit()`
- `POST /admin/franchises/<int:franchise_id>/messaging` -> `save_messaging_account()`
- `POST /admin/franchises/<int:franchise_id>/messaging/<int:account_id>/disable` -> `disable_messaging_account()`
- `GET /admin/franchises/<int:franchise_id>/meta/signup/start` -> `meta_signup_start()`
- `GET /admin/meta/signup/callback` -> `meta_signup_callback()`
- `GET,POST /manage/branches` -> `manage_branches()`
- `POST /manage/branches/<int:branch_id>/move` -> `move_branch()`
- `GET,POST /manage/users` -> `manage_users()`
- `POST /manage/users/<int:user_id>/assign` -> `assign_user()`
- `POST /manage/users/<int:user_id>/toggle` -> `toggle_user()`
- `POST /manage/users/<int:user_id>/password` -> `reset_user_password()`
- `GET /manage/credentials` -> `manage_credentials()`
- `POST /manage/credentials/reset-all` -> `reset_all_passwords()`
- `GET,POST /manage/prices` -> `manage_prices()`
- `GET,POST /chatbot/inbox` -> `chatbot_inbox()`

Billing/webhooks:

- `POST /billing/close-month` -> `close_billing_month()`
- `POST /billing/<int:billing_id>/payment` -> `update_billing_payment()`
- `POST /billing/<int:billing_id>/payment-link` -> `generate_payment_link()`
- `POST /webhook/paystack` -> `paystack_webhook()`
- `GET,POST /webhooks/meta/<franchise_slug>/<branch_slug>/<token>` -> `meta_webhook()`

## Required Environment Variables

Boot-critical:

- `DATABASE_URL` or `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`
- `SECRET_KEY`

Messaging/Meta:

- `META_APP_ID`
- `META_APP_SECRET`
- `META_ACCESS_TOKEN`
- `WHATSAPP_BUSINESS_ACCOUNT_ID`
- `WHATSAPP_PHONE_NUMBER_ID`
- `VERIFY_TOKEN`
- `META_EMBEDDED_SIGNUP_REDIRECT_URI`
- `MESSAGING_TOKEN_ENCRYPTION_KEY`
- `ALLOW_PLAINTEXT_MESSAGING_TOKENS` only for emergency migration compatibility.

Paystack:

- `PAYSTACK_SECRET_KEY`
- `PAYSTACK_WEBHOOK_SECRET`

Optional/runtime:

- `OPENAI_API_KEY`
- `SESSION_COOKIE_SECURE`
- `SESSION_COOKIE_SAMESITE`
- `SESSION_LIFETIME_HOURS`
- `LOG_LEVEL`
- `DEFAULT_RATE_LIMIT`
- `RATELIMIT_STORAGE_URI`
- `API_TOKEN_MAX_AGE_SECONDS`
- `FRONTEND_API_TOKEN`
- `ALLOW_PUBLIC_DASHBOARD_API`
- `FRONTEND_ORIGIN`
- `SKIP_DATABASE_INIT`
- `SKIP_ALEMBIC_MIGRATIONS`
- `STRICT_ALEMBIC_MIGRATIONS`
- `REQUIRE_DATABASE_URL`
- `RAILWAY_ENVIRONMENT`
- `RAILWAY_SERVICE_ID`
- `FLASK_ENV`
- `APP_ENV`
- `PGPOOL_MINCONN`
- `PGPOOL_MAXCONN`
- `PGCONNECT_TIMEOUT`
- `SUPERADMIN_USERNAME`
- `SUPERADMIN_PASSWORD`
- `SUPERADMIN_NAME`
- `DEMO_SUPERADMIN_USERNAME`
- `DEMO_SUPERADMIN_PASSWORD`
- `DEMO_RECEPTION_USERNAME`
- `DEMO_RECEPTION_PASSWORD`
- `DEMO_FRANCHISE_USERNAME`
- `DEMO_FRANCHISE_PASSWORD`
- `RUN_LEGACY_BOOTSTRAP`
- `PUBLIC_BASE_URL`
- `BILLING_EMAIL`
- `ADMIN_EMAIL`
- `AUTOMATION_WORKER_INTERVAL_SECONDS`
- `AUTOMATION_WORKER_BATCH_SIZE`

## Operating Rules

1. Never deploy production without PostgreSQL.
2. Never enable `ALLOW_PUBLIC_DASHBOARD_API` in production.
3. Never expose `messaging_accounts.access_token`.
4. Never use `reset_live_passwords.py` in production unless the temporary password is changed first and access is controlled.
5. Keep `web`, `worker`, and `scheduler` running for reliable booking confirmations, reminders, and automations.
6. Keep `PAYSTACK_WEBHOOK_SECRET` set so Paystack webhooks do not fall back to only the secret key.
7. Meta webhooks must use the route format in `meta_webhook()`: `/webhooks/meta/<franchise_slug>/<branch_slug>/<token>`.
8. Client isolation relies on `franchise_id` and `branch_id` checks in `platform_helpers.scope_clause()`, `selected_branch_for_user()`, and `assert_tenant_scope()`.

## Scores

These scores are based on repository state after the latest documentation generation:

- OWNER_READINESS_SCORE: 84/100
- PRODUCTION_READINESS_SCORE: 82/100
- META_READINESS_SCORE: 78/100
- SECURITY_SCORE: 76/100
- SCALABILITY_SCORE: 72/100

Recommendation: GO for controlled production launch after live Meta Embedded Signup is tested with a real WABA and after demo bootstrap/known-password reset procedures are locked down.
