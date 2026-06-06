# Operations Manual

## Daily Operations

1. Check `/health`.
2. Check `/health/db`.
3. Check Railway web logs.
4. Check worker logs for processed jobs.
5. Check scheduler logs.
6. Open `/admin/client-audit`.
7. Review failed messages.
8. Review `failed_jobs`.
9. Review Paystack webhook events.
10. Confirm Meta webhook events are arriving for active clients.

## Weekly Operations

1. Review `communication_logs` failed statuses.
2. Review `scheduled_jobs` stuck in `running`.
3. Review `webhook_events` volume.
4. Review new audit logs.
5. Confirm PostgreSQL backups are current.
6. Confirm all services are running.

## Monthly Operations

1. Run billing close:

```text
POST /billing/close-month
```

2. Generate payment links for unpaid billing records.
3. Reconcile Paystack dashboard with `billing_records`.
4. Check subscriptions nearing expiry.
5. Review overage usage.
6. Verify scheduler sent service/work reminders.

## Quarterly Operations

1. Rotate critical secrets where practical.
2. Review Meta tokens.
3. Review Paystack webhook secret.
4. Review user list and disable unused accounts.
5. Review audit logs.
6. Validate database backups by restoring to staging.
7. Review roadmap and security findings.

## New Client Onboarding

Follow `NEW_CLIENT_PLAYBOOK.md`.

Core steps:

1. Create franchise.
2. Create branch.
3. Create users.
4. Configure service prices.
5. Configure Meta account.
6. Configure billing.
7. Test public booking.
8. Test login.
9. Test messaging.
10. Activate subscription.

## Client Offboarding

1. Set franchise inactive.
2. Set subscription inactive/cancelled.
3. Disable users.
4. Disable messaging account.
5. Keep data for audit.
6. Stop billing/payment link generation.

## Number Migration

1. Get new Meta phone number ID.
2. Open `/admin/client-audit`.
3. Edit messaging account.
4. Update `phone_number_id`.
5. Rotate token if needed.
6. Update webhook in Meta if URL/token changed.
7. Test inbound and outbound.

## Token Rotation

Meta token:

1. Generate new token in Meta.
2. Open `/admin/client-audit`.
3. Edit account.
4. Enter new token in Rotate Token field.
5. Save.
6. Confirm version `v2` and age `0 days`.
7. Send test message.

Paystack:

1. Rotate in Paystack.
2. Update Railway env.
3. Redeploy/restart web.
4. Test webhook signature with a Paystack test event.

## User Management

Create users:

- `/manage/users`

Assign role/scope:

- `POST /manage/users/<id>/assign`

Disable:

- `POST /manage/users/<id>/toggle`

Reset password:

- `POST /manage/users/<id>/password`

Avoid:

- platform-wide reset except emergency.

## Billing Reconciliation

1. Compare Paystack successful transactions with `billing_records`.
2. Confirm `payment_reference_id`.
3. Confirm `franchises.subscription_status`.
4. Confirm `subscription_end`.
5. Manually mark paid only if Paystack dashboard confirms payment.

## Automation Verification

1. Create test booking.
2. Confirm `automation_engine.emit_event()` creates `scheduled_jobs`.
3. Confirm worker completes job.
4. Confirm `communication_logs`.
5. Confirm no failed job.

## Backup Verification

1. Export production backup.
2. Restore to staging.
3. Run app against staging.
4. Login as test superadmin.
5. Check bookings and billing records.

## Security Reviews

Review:

- active users
- superadmins
- API tokens
- Railway env variables
- Meta tokens
- Paystack keys
- `ALLOW_PUBLIC_DASHBOARD_API`
- hardcoded reset scripts
- audit logs

## Support Procedures

Booking not showing:

1. Check user role.
2. Check `franchise_id` and `branch_id`.
3. Use superadmin to confirm booking exists.

Message not sent:

1. Check WhatsApp opt-in.
2. Check subscription active.
3. Check messaging account active.
4. Check token status.
5. Check `communication_logs`.

Webhook not received:

1. Check Meta/Paystack webhook URL.
2. Check secret/token.
3. Check logs.
4. Check webhook event tables.

Payment not applied:

1. Check Paystack dashboard.
2. Check `paystack_webhook_events`.
3. Check `billing_records`.
4. Manually mark paid only with proof.
