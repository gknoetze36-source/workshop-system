# Railway Deployment

## PostgreSQL Only

Set this on every Railway service:

```txt
DATABASE_URL=${{Postgres.DATABASE_URL}}
REQUIRE_DATABASE_URL=true
```

With `REQUIRE_DATABASE_URL=true`, the app refuses to use local SQLite when `DATABASE_URL` is missing.

## Services

Create one Railway project with one PostgreSQL database and these app services from the same GitHub repo.

| Service | Type | Start command |
|---|---|---|
| `web` | Web | `gunicorn app:app --bind 0.0.0.0:$PORT` |
| `automation-worker` | Worker | `python automation_worker.py` |
| `scheduler` | Worker | `python scheduler.py` |
| `daily-jobs` | Cron | `python cron_jobs.py daily` |
| `billing-close` | Cron | `python cron_jobs.py billing` |
| `subscription-check` | Cron | `python cron_jobs.py subscriptions` |

Suggested cron timing:

```txt
daily-jobs: once daily at 08:00
billing-close: once daily at 00:15
subscription-check: once daily at 00:05
```

The `Procfile` is kept for simple process discovery, but Railway should use the explicit commands above.

## Required Variables

Set on all app services:

```txt
DATABASE_URL=${{Postgres.DATABASE_URL}}
REQUIRE_DATABASE_URL=true
SECRET_KEY=<strong-random-secret>
PUBLIC_BASE_URL=https://your-production-domain
AUTOMATION_WORKER_INTERVAL_SECONDS=30
AUTOMATION_WORKER_BATCH_SIZE=50
PGCONNECT_TIMEOUT=5
```

Set where messaging is needed:

```txt
TWILIO_ACCOUNT_SID=
TWILIO_AUTH_TOKEN=
TWILIO_SMS_FROM=
TWILIO_WHATSAPP_FROM=
```

Optional:

```txt
OPENAI_API_KEY=
```

## Deploy Order

1. Create Railway PostgreSQL.
2. Connect GitHub repo.
3. Create `web` service.
4. Add all required variables.
5. Run once from Railway shell:

```txt
python setup_db.py
```

6. Start `automation-worker`.
7. Start `scheduler`.
8. Add cron services.
9. Open:

```txt
/health
/health/db
```

Both must return `200`.

## External Entry Points

Internal portal:

```txt
/
/login
/dashboard
```

External booking webhook:

```txt
POST /webhook/booking/<franchise_slug>/<branch_slug>/<token>
```

Twilio webhook:

```txt
POST /webhook/twilio/<franchise_slug>/<branch_slug>/<token>
```

Stripe webhook placeholder:

```txt
POST /webhook/stripe
```

## Backups

Enable Railway PostgreSQL backups before production use.

Before schema changes:

```txt
python setup_db.py
```

Then confirm:

```txt
/health/db
```
