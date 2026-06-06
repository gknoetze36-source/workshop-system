# Disaster Recovery Playbook

## Database Corruption

Symptoms:

- `/health/db` fails
- app logs `database_initialization_failed`
- worker/scheduler crash
- data inconsistencies

Procedure:

1. Stop worker and scheduler.
2. Put web in maintenance or restrict access.
3. Take immediate snapshot of current DB.
4. Restore latest known-good Railway PostgreSQL backup.
5. Deploy same commit SHA as backup time if possible.
6. Run `python deployment_check.py`.
7. Run `/health/db`.
8. Restart worker and scheduler.
9. Validate `users`, `franchises`, `branches`, `bookings`, `messaging_accounts`, `billing_records`.

## Meta Token Loss

Symptoms:

- outbound WhatsApp fails
- `communication_logs.status` starts with `failed`
- token decryption errors

Procedure:

1. Confirm `MESSAGING_TOKEN_ENCRYPTION_KEY` is unchanged.
2. If key was lost, old encrypted tokens cannot be decrypted.
3. Open `/admin/client-audit`.
4. Rotate token by editing messaging account and entering new access token.
5. Confirm token version shows `v2`.
6. Send test message.

## Webhook Failure

Meta:

1. Confirm URL:

```text
/webhooks/meta/<franchise_slug>/<branch_slug>/<token>
```

2. Confirm `franchises.inbound_webhook_token`.
3. Confirm `messaging_accounts.webhook_verify_token`.
4. Confirm `messaging_accounts.webhook_secret`.
5. Inspect `webhook_events`.

Paystack:

1. Confirm URL:

```text
/webhook/paystack
```

2. Confirm `PAYSTACK_WEBHOOK_SECRET`.
3. Inspect `paystack_webhook_events`.
4. Verify payment manually in Paystack dashboard.
5. If payment succeeded but webhook failed, use manual billing update route.

## Worker Failure

1. Restart Railway worker.
2. Inspect `scheduled_jobs` for stuck `running`.
3. Reset stale `running` jobs to `pending` only after confirming no worker is processing them.
4. Inspect `failed_jobs`.
5. Retry failed jobs through admin route.

## Scheduler Failure

1. Restart scheduler service.
2. Run urgent jobs manually:

```bash
python cron_jobs.py same-day
python cron_jobs.py day-before
python cron_jobs.py inquiry
python cron_jobs.py missed
python cron_jobs.py yearly
```

3. Inspect `reminder_campaigns` and `communication_logs`.

## Railway Outage

1. Check Railway status.
2. Avoid repeated migrations during outage.
3. Export latest DB backup if accessible.
4. If prolonged, redeploy repo to new Railway project:
   - add PostgreSQL
   - set env vars
   - restore DB
   - deploy web/worker/scheduler/billing
   - update DNS/webhook URLs

## Credential Compromise

If `SECRET_KEY` compromised:

1. Rotate `SECRET_KEY`.
2. Force all users to log in again.
3. Consider resetting all passwords.

If `MESSAGING_TOKEN_ENCRYPTION_KEY` compromised:

1. Rotate all Meta access tokens.
2. Change encryption key.
3. Re-save all messaging accounts with new tokens.

If Paystack secret compromised:

1. Rotate in Paystack.
2. Update Railway env.
3. Verify webhook signatures.

If Meta app secret compromised:

1. Rotate in Meta.
2. Update Railway env and `messaging_accounts.webhook_secret` if used.
3. Test webhook and outbound.

## Billing Outage

If Paystack unavailable:

1. Stop generating new payment links.
2. Continue operating existing clients manually.
3. Record manual payments in `/billing/<id>/payment`.
4. Reconcile when Paystack returns.

## Client Data Recovery

1. Identify franchise ID.
2. Export rows from:
   - `franchises`
   - `branches`
   - `users`
   - `customers`
   - `bookings`
   - `reminder_campaigns`
   - `communication_logs`
   - `billing_records`
   - `messaging_accounts`
3. Restore into staging first.
4. Validate tenant isolation.
5. Restore production only after owner approval.

## Environment Rebuild

Minimum required:

- GitHub repo
- Railway project
- PostgreSQL backup
- all env vars
- custom domain
- Meta app access
- Paystack account access

Steps:

1. Create Railway project.
2. Add PostgreSQL.
3. Restore DB.
4. Deploy web service.
5. Set env vars.
6. Run `deployment_check.py`.
7. Deploy worker/scheduler/billing.
8. Update Meta and Paystack webhook URLs if domain changed.
9. Verify login/bookings/messaging/billing.
