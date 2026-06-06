# Railway Operations Guide

Deployment files:

- `railway.json`
- `Procfile`
- `Dockerfile`
- `requirements.txt`
- `deployment_check.py`
- `RAILWAY_DEPLOYMENT.md`

## Services

### web

Command from `railway.json`:

```bash
gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers ${WEB_CONCURRENCY:-1} --threads ${GUNICORN_THREADS:-2} --timeout ${GUNICORN_TIMEOUT:-60}
```

Responsibilities:

- Flask UI
- API routes
- auth
- public bookings
- Meta webhooks
- Paystack webhooks
- superadmin UI

### worker

Command from `Procfile`:

```bash
python automation_worker.py
```

Responsibilities:

- process `scheduled_jobs`
- send automation messages
- write `automation_logs`
- write `failed_jobs`

### scheduler

Command:

```bash
python scheduler.py
```

Responsibilities:

- reminders
- inquiry followups
- missed booking followups
- declined work reminders
- yearly/service reminders

### billing

Command:

```bash
python cron_jobs.py billing
```

Responsibilities:

- close billing period if configured as Railway cron

## Environment Variables

Boot-critical:

- `DATABASE_URL`
- `SECRET_KEY`

Postgres fallback:

- `PGHOST`
- `PGPORT`
- `PGDATABASE`
- `PGUSER`
- `PGPASSWORD`

Meta:

- `META_APP_ID`
- `META_APP_SECRET`
- `META_ACCESS_TOKEN`
- `WHATSAPP_BUSINESS_ACCOUNT_ID`
- `WHATSAPP_PHONE_NUMBER_ID`
- `VERIFY_TOKEN`
- `META_EMBEDDED_SIGNUP_REDIRECT_URI`
- `MESSAGING_TOKEN_ENCRYPTION_KEY`

Paystack:

- `PAYSTACK_SECRET_KEY`
- `PAYSTACK_WEBHOOK_SECRET`

Recommended:

- `REQUIRE_DATABASE_URL=true`
- `WEB_CONCURRENCY=1`
- `GUNICORN_THREADS=2`
- `GUNICORN_TIMEOUT=60`
- `LOG_LEVEL=INFO`
- `RATELIMIT_STORAGE_URI`
- `FRONTEND_ORIGIN`
- `BILLING_EMAIL`
- `ADMIN_EMAIL`

Never enable in production:

- `ALLOW_PUBLIC_DASHBOARD_API=true`
- `ALLOW_PLAINTEXT_MESSAGING_TOKENS=true` except temporary emergency migration.

## Brand-New Production Deployment

1. Create Railway project.
2. Add Railway PostgreSQL.
3. Create web service from GitHub repository.
4. Set variables above on web service.
5. Deploy web.
6. Open web shell and run:

```bash
python deployment_check.py
```

7. Confirm:

```text
GET /health
GET /health/db
```

8. Create worker service from same repo with command `python automation_worker.py`.
9. Create scheduler service from same repo with command `python scheduler.py`.
10. Create billing cron/service if required with `python cron_jobs.py billing`.
11. Create first superadmin with `SUPERADMIN_USERNAME` and `SUPERADMIN_PASSWORD`, or use generated startup account then reset immediately.
12. Configure custom domain.
13. Configure Meta webhook URL.
14. Configure Paystack webhook URL.

## Health Checks

Lightweight:

- `GET /health`

Database:

- `GET /health/db`

System status:

- `GET /admin/system-status`

Deployment script:

- `python deployment_check.py`

## Monitoring

Watch Railway logs for:

- `startup_environment_validated`
- `startup_success`
- `database_initialization_failed`
- `meta_webhook_invalid_signature`
- `paystack_webhook_invalid_signature`
- worker print output
- scheduler print output

Watch DB tables:

- `failed_jobs`
- `scheduled_jobs`
- `communication_logs`
- `webhook_events`
- `paystack_webhook_events`
- `billing_records`

## Scaling

Low-cost defaults:

- web workers: 1
- gunicorn threads: 2
- PostgreSQL pool max: 5

Scale path:

1. Increase `PGPOOL_MAXCONN`.
2. Increase `GUNICORN_THREADS`.
3. Increase worker replicas only after reviewing job locking.
4. Move queue from DB polling to durable queue when load requires.
5. Add Redis-backed rate limit storage through `RATELIMIT_STORAGE_URI`.

## Backups

Railway PostgreSQL must have automated backups enabled.

Manual backup before major migration:

1. Use Railway database backup/export.
2. Record current commit SHA.
3. Record current env vars excluding secrets.
4. Run migration.

## Rollback

Code rollback:

1. Roll Railway deployment back to previous commit.
2. Restart web/worker/scheduler.
3. Run `/health` and `/health/db`.

Migration rollback:

1. Confirm backup exists.
2. Run Alembic downgrade only if needed.
3. Do not drop audit/payment webhook tables unless required.

## Recovery

If web fails boot:

1. Check `DATABASE_URL`/`PG*`.
2. Check `SECRET_KEY`.
3. Check dependency installation from `requirements.txt`.
4. Check migration error logs.

If worker fails:

1. Confirm `DATABASE_URL`.
2. Restart worker.
3. Inspect `scheduled_jobs`.

If scheduler fails:

1. Restart scheduler.
2. Run missed cron manually if needed.

If Paystack webhook fails:

1. Check `PAYSTACK_WEBHOOK_SECRET`.
2. Check `/webhook/paystack` URL.
3. Inspect `paystack_webhook_events`.

If Meta webhook fails:

1. Check route token in URL.
2. Check `webhook_secret`.
3. Check `webhook_verify_token`.
4. Inspect `webhook_events`.
