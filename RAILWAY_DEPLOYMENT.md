# Railway Deployment

## Services

Create four Railway services from this repository.

### web

Start command:

```bash
gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers ${WEB_CONCURRENCY:-1} --threads ${GUNICORN_THREADS:-2} --timeout ${GUNICORN_TIMEOUT:-60}
```

Responsibilities:
- Flask UI and API
- Auth
- Public bookings
- Meta webhook
- Paystack webhook
- Superadmin organization and audit pages

### worker

Start command:

```bash
python automation_worker.py
```

Responsibilities:
- Processes `scheduled_jobs`
- Retries automation jobs through database state

### scheduler

Start command:

```bash
python scheduler.py
```

Responsibilities:
- Same-day booking reminders
- Day-before reminders
- Inquiry follow-ups
- Missed booking follow-ups
- Declined work reminders
- Yearly/service reminders

### billing

Start command:

```bash
python cron_jobs.py billing
```

Use as Railway cron if you do not want billing handled manually.

## Required Variables

Boot-critical:

```txt
DATABASE_URL
SECRET_KEY
```

Railway Postgres fallback is supported if `DATABASE_URL` is absent:

```txt
PGHOST
PGPORT
PGDATABASE
PGUSER
PGPASSWORD
```

Meta:

```txt
META_APP_ID
META_APP_SECRET
META_ACCESS_TOKEN
WHATSAPP_BUSINESS_ACCOUNT_ID
WHATSAPP_PHONE_NUMBER_ID
VERIFY_TOKEN
META_EMBEDDED_SIGNUP_REDIRECT_URI
MESSAGING_TOKEN_ENCRYPTION_KEY
```

Paystack:

```txt
PAYSTACK_SECRET_KEY
PAYSTACK_WEBHOOK_SECRET
```

Optional:

```txt
OPENAI_API_KEY
FRONTEND_ORIGIN
FRONTEND_API_TOKEN
RATELIMIT_STORAGE_URI
WEB_CONCURRENCY=1
GUNICORN_THREADS=2
GUNICORN_TIMEOUT=60
LOG_LEVEL=INFO
```

## Database

The runtime schema authority is:

```txt
database.py
database/migrations/versions/
```

Run migrations on deploy through app startup unless `SKIP_ALEMBIC_MIGRATIONS=true`.

## Meta Setup

Webhook URL:

```txt
https://<domain>/webhooks/meta/<franchise_slug>/<branch_slug>/<inbound_webhook_token>
```

Messaging accounts are stored in `messaging_accounts`.

Tokens are stored encrypted. Do not paste tokens into docs or screenshots.

Embedded Signup callback:

```txt
https://<domain>/admin/meta/signup/callback
```

Set this as `META_EMBEDDED_SIGNUP_REDIRECT_URI`.

## Paystack Setup

Webhook URL:

```txt
https://<domain>/webhook/paystack
```

The app validates signatures and stores webhook events in `paystack_webhook_events` to prevent duplicate processing.

## Health Checks

Lightweight:

```txt
GET /health
```

Database:

```txt
GET /health/db
```

Deployment check:

```bash
python deployment_check.py
```

## Rollback

1. Roll back the Railway deployment to the previous commit.
2. If needed, run Alembic downgrade for the latest migration.
3. Keep `messaging_accounts`, `webhook_events`, `billing_records`, and legacy `whatsapp_numbers`.
4. Do not delete audit or webhook idempotency records during rollback.
