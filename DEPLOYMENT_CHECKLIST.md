# Deployment Checklist

## Required Railway Variables

```txt
DATABASE_URL
SECRET_KEY
META_APP_ID
META_APP_SECRET
META_ACCESS_TOKEN
WHATSAPP_BUSINESS_ACCOUNT_ID
WHATSAPP_PHONE_NUMBER_ID
VERIFY_TOKEN
OPENAI_API_KEY
MESSAGING_TOKEN_ENCRYPTION_KEY
```

## Optional Runtime Tuning

```txt
WEB_CONCURRENCY=1
GUNICORN_THREADS=2
GUNICORN_TIMEOUT=60
LOG_LEVEL=INFO
```

## Pre-Deploy Check

Run locally or in Railway shell:

```bash
python deployment_check.py
```

This only validates local environment values and URL format. It does not call Meta, OpenAI, or the database.

## Deploy

1. Set the required Railway variables.
2. Deploy the web service.
3. Confirm:

```txt
GET /health
```

Expected:

```json
{"status":"ok"}
```

4. Sign in as super admin and check:

```txt
GET /admin/system-status
```

The page reports whether Meta variables are configured without calling external services.
