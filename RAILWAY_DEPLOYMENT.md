# VANTA Railway Deployment

This guide deploys the full production backend on Railway with one shared PostgreSQL database, one web service, one automation worker, one scheduler, and optional cron jobs.

## 1. Railway Project

Create one Railway project:

```txt
VANTA Production
```

Inside this project create:

```txt
Postgres
web
automation-worker
scheduler
billing-close
subscription-check
```

Use the same GitHub repo for every app service:

```txt
gknoetze36-source/workshop-system
```

Do not create separate databases per client. Every workshop uses the same database with tenant isolation through `franchise_id`, `branch_id`, and workshop-specific messaging accounts.

## 2. PostgreSQL

In Railway:

1. Click `New`.
2. Choose `Database`.
3. Choose `PostgreSQL`.
4. Name it:

```txt
Postgres
```

5. Open the Postgres service.
6. Confirm Railway exposes:

```txt
DATABASE_URL
```

7. Enable backups before production traffic.

Use this database for every backend service.

## 3. Web Service

Create a Railway service from GitHub.

Service name:

```txt
web
```

Source:

```txt
gknoetze36-source/workshop-system
```

Start command:

```bash
gunicorn app:app --bind 0.0.0.0:${PORT:-8080}
```

This service handles:

```txt
login
dashboard
booking APIs
public booking webhooks
Paystack webhook
health checks
```

## 4. Automation Worker Service

Create another Railway service from the same GitHub repo.

Service name:

```txt
automation-worker
```

Start command:

```bash
python automation_worker.py
```

This service must run all the time. It processes:

```txt
booking confirmations
automation_rules
scheduled_jobs
failed job retries
future AI automation jobs
```

Without this service, booking confirmations and automation jobs will not be reliable.

## 5. Scheduler Service

Create another Railway service from the same GitHub repo.

Service name:

```txt
scheduler
```

Start command:

```bash
python scheduler.py
```

This service must run all the time. It handles:

```txt
same-day booking reminders
day-before booking reminders
service reminders
end-of-month work reminders
yearly service reminders
missed booking follow-ups
declined work reminders
inquiry follow-ups
```

Reminder timing uses South African time:

| Job | SAST | UTC |
|---|---:|---:|
| Same-day booking reminder | 07:00 | 05:00 |
| Day-before booking reminder | 08:00 | 06:00 |
| Service/yearly/work reminders | 09:00 | 07:00 |
| Missed booking + declined work | 18:00 | 16:00 |
| Inquiry follow-ups | Every 5 minutes, 07:00-18:00 SAST | UTC+2 adjusted |

South Africa does not use daylight saving time, so UTC+2 is safe for now.

## 6. Optional Cron Services

These are optional because `scheduler` already runs the recurring reminder jobs. Use them only if you prefer Railway cron jobs instead of the always-on scheduler.

### Billing Close

Service name:

```txt
billing-close
```

Start command:

```bash
python cron_jobs.py billing
```

Schedule:

```txt
Daily at 00:15 SAST
```

### Subscription Check

Service name:

```txt
subscription-check
```

Start command:

```bash
python cron_jobs.py subscriptions
```

Schedule:

```txt
Daily at 00:05 SAST
```

Do not run duplicate reminder cron jobs if the always-on `scheduler` service is active, or customers may receive duplicated messages if safeguards are bypassed by future changes.

## 7. Required Environment Variables

Set these on every backend service:

```txt
DATABASE_URL=${{Postgres.DATABASE_URL}}
REQUIRE_DATABASE_URL=true
SECRET_KEY=<strong-random-secret>
PUBLIC_BASE_URL=https://<your-railway-web-domain>
PGCONNECT_TIMEOUT=5
AUTOMATION_WORKER_INTERVAL_SECONDS=30
AUTOMATION_WORKER_BATCH_SIZE=50
```

Set this once before first production login:

```txt
SUPERADMIN_PASSWORD=<strong-superadmin-password>
```

Keep it set until the first deployment has bootstrapped the admin account. After confirming login works, rotate/remove it according to your admin policy.

## 8. Messaging

WhatsApp messaging uses the Meta WhatsApp Cloud API. The app keeps manual SMS action links for staff, but direct SMS sending is not configured.

For each workshop, create one active `messaging_accounts` row:

```txt
provider=meta
channel=whatsapp
business_account_id=<meta-business-account-id>
whatsapp_business_account_id=<waba-id>
phone_number_id=<meta-phone-number-id>
sender_id=<workshop-whatsapp-number>
access_token=<meta-access-token>
token_expiry=<token-expiry-iso-timestamp>
webhook_verify_token=<meta-verify-token>
webhook_secret=<meta-app-secret>
embedded_signup_state=not_started
coexistence_status=not_started
is_active=true
```

Set this on every service that sends or receives WhatsApp messages:

```txt
MESSAGING_TOKEN_ENCRYPTION_KEY=<fernet-key>
```

Meta webhook URL:

```txt
https://<domain>/webhooks/meta/<franchise_slug>/<branch_slug>/<token>
```

## 9. Paystack Billing Variables

Set on the web service and any billing-related service:

```txt
PAYSTACK_SECRET_KEY=<paystack-secret-key>
PAYSTACK_WEBHOOK_SECRET=<paystack-webhook-secret>
```

Paystack webhook URL:

```txt
https://<your-railway-web-domain>/webhook/paystack
```

## 10. Optional AI Variables

Set only if AI features are enabled:

```txt
OPENAI_API_KEY=<openai-api-key>
```

## 11. First Deployment Order

Deploy in this order:

1. Create Railway project.
2. Add PostgreSQL.
3. Create `web` service.
4. Add all required environment variables to `web`.
5. Deploy `web`.
6. Open the Railway shell for `web`.
7. Run:

```bash
python setup_db.py
```

8. Open:

```txt
https://<your-railway-web-domain>/health
https://<your-railway-web-domain>/health/db
```

Both should return success.

9. Create `automation-worker`.
10. Copy the same env vars from `web`.
11. Deploy `automation-worker`.
12. Create `scheduler`.
13. Copy the same env vars from `web`.
14. Deploy `scheduler`.
15. Add optional billing/subscription cron services.
16. Test login and create one test booking.
17. Confirm `scheduled_jobs`, `reminder_campaigns`, and `communication_logs` populate.

## 12. Health Checks

Check:

```txt
GET /health
GET /health/db
```

Expected:

```txt
200 OK
database connected
```

If `/health/db` fails, check:

```txt
DATABASE_URL=${{Postgres.DATABASE_URL}}
REQUIRE_DATABASE_URL=true
```

## 13. Domains

In Railway web service:

1. Go to `Settings`.
2. Open `Networking`.
3. Generate a Railway domain or attach your custom domain.
4. Set:

```txt
PUBLIC_BASE_URL=https://<your-production-domain>
```

Use this URL for:

```txt
Paystack webhook callback
frontend backend URL
```

## 14. Webhook URLs

Paystack:

```txt
POST https://<domain>/webhook/paystack
```

Public booking:

```txt
POST https://<domain>/webhook/booking/<franchise_slug>/<branch_slug>/<token>
```

Meta WhatsApp Cloud API:

```txt
GET/POST https://<domain>/webhooks/meta/<franchise_slug>/<branch_slug>/<token>
```

## 15. Frontend/Vercel Connection

For the Vercel frontend, set:

```txt
NEXT_PUBLIC_BACKEND_URL=https://<your-railway-web-domain>
BACKEND_API_URL=https://<your-railway-web-domain>
```

If the frontend uses auth cookies or redirects, make sure the frontend production URL is allowed by the backend CORS/session settings if those settings are added later.

## 16. Production Verification

After all services are running:

1. Log in as super admin.
2. Create or confirm a franchise/workshop.
3. Create a branch.
4. Add a messaging account.
5. Create a test booking.
6. Confirm a `booking.created` automation job is created.
7. Confirm `automation-worker` processes it.
8. Confirm `communication_logs` records the confirmation.
9. Run the scheduler long enough to cross a reminder window, or run:

```bash
python cron_jobs.py day-before
python cron_jobs.py same-day
python cron_jobs.py yearly
python cron_jobs.py missed
```

10. Confirm `reminder_campaigns` records reminders.

## 17. Common Faults

### App falls back to SQLite

Fix:

```txt
DATABASE_URL=${{Postgres.DATABASE_URL}}
REQUIRE_DATABASE_URL=true
```

### Confirmations do not send

Check:

```txt
automation-worker is deployed
automation-worker has DATABASE_URL
automation-worker logs show "Automation worker started"
```

### Reminders do not send

Check:

```txt
scheduler is deployed
scheduler has DATABASE_URL
scheduler logs show "Scheduler started..."
customer has reminder_opt_in=true
booking has phone number
workshop/franchise is active
branch is active
```

### Postgres connection errors

Check:

```txt
DATABASE_URL is copied from the Railway Postgres service
PGCONNECT_TIMEOUT=5
No accidental duplicated postgres URL text is pasted into DATABASE_URL
```

## 18. Minimum Production Services

Required:

```txt
Postgres
web
automation-worker
scheduler
```

Optional:

```txt
billing-close
subscription-check
```

Do not ship production with only the `web` service. The system will load, but confirmations, reminders, and automations will not be reliable.
