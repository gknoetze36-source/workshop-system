# New Client Playbook

This playbook describes how to onboard a new VANTA client using actual repository routes and functions.

## Roles Required

Superadmin is required for:

- creating franchises
- editing subscription and plan details
- moving branches
- creating superadmin users
- Meta messaging setup
- Paystack billing actions

Franchise admin can:

- manage own branches
- manage own users
- manage service prices
- view own bookings/customers/reminders/reports/chatbot inbox

Reception can:

- manage branch-scoped bookings
- view branch-scoped customers/reminders

## New Franchise Setup

Route:

- `GET,POST /manage/franchises`

Template:

- `templates/manage_franchises.html`

Function:

- `app.manage_franchises()`

Required form fields:

- `name`
- `industry`
- `plan_code`
- `contact_email`
- `contact_phone`
- `setup_fee`
- `monthly_base_price`
- `monthly_message_limit`
- `overage_price_per_message`
- `public_base_url`
- `inbound_webhook_token`
- `notes`

Database changes:

- inserts into `franchises`
- then calls `platform_helpers.provision_business()`

Provisioning creates or updates:

- `feature_flags`
- `services`
- `automation_rules`
- `onboarding_sessions`
- `onboarding_state`

Audit:

- `record_audit("franchise_created", "franchise", ...)`

## New Workshop / SaaS Root Setup

Runtime setup is franchise-centric. `database._ensure_workshop_mappings()` ensures each franchise has a `workshops` row and `franchises.workshop_id`.

Do not manually create workshop rows unless you understand the mapping. Use `/manage/franchises` and let `initialize_database()`/mapping logic maintain `workshop_id`.

## New Branch Setup

Route:

- `GET,POST /manage/branches`

Function:

- `app.manage_branches()`

Required form fields:

- `franchise_id`
- `name`
- `code`
- `location`
- `contact_email`
- `contact_phone`
- `public_booking_enabled`

Database changes:

- inserts into `branches`

Audit:

- `record_audit("branch_created", "branch", ...)`

Branch limits:

- enforced by `platform_helpers.can_add_branch()`
- based on `franchises.branch_limit`

## New User Setup

Route:

- `GET,POST /manage/users`

Function:

- `app.manage_users()`

Template:

- `templates/manage_users.html`

Form fields:

- `username`
- `password`
- `full_name`
- `email`
- `role`
- `phone`
- `franchise_id`
- `branch_id`

Rules:

- reception users must have a branch
- franchise admin users belong to a franchise
- superadmin can create `super_admin`, `franchise_admin`, `reception`
- franchise admin can create `franchise_admin`, `reception`

Database changes:

- inserts into `users`
- stores hash in `password_hash`
- clears plaintext `password`

Audit:

- `record_audit("user_created", "user", ...)`

## Role Assignment

Route:

- `POST /manage/users/<int:user_id>/assign`

Function:

- `app.assign_user()`

Rules:

- franchise admin cannot modify users outside own franchise
- reception must be assigned to visible branch

Audit:

- `record_audit("user_assignment_updated", "user", ...)`

## Service Creation and Pricing Setup

Route:

- `GET,POST /manage/prices`

Function:

- `app.manage_prices()`

Database:

- inserts into `service_prices`

Audit:

- `record_audit("service_price_created", "service_price", ...)`

Pricing lookup:

- `platform_helpers.find_service_price()`

## Automation Setup

Primary provisioning:

- `platform_helpers.provision_business()`

Template source:

- `industry_templates`
- `automation_templates`

Runtime rules:

- `automation_rules`

Manual retry:

- `POST /admin/failed-jobs/<int:failed_job_id>/retry`

## Billing Setup

Franchise billing fields are managed in `/manage/franchises`:

- `setup_fee`
- `monthly_base_price`
- `monthly_message_limit`
- `overage_price_per_message`
- `subscription_status`
- `subscription_start`
- `subscription_end`

Close month:

- `POST /billing/close-month`
- function `close_billing_month()`
- helper `close_billing_period()`

Generate payment link:

- `POST /billing/<int:billing_id>/payment-link`
- function `generate_payment_link()`
- helper `create_payment_link()`

Manual payment update:

- `POST /billing/<int:billing_id>/payment`
- function `update_billing_payment()`

## Meta Number Setup

Manual setup:

1. Open `/admin/client-audit`.
2. Use "Create Messaging Account" form.
3. Enter:
   - Business Account ID
   - WABA ID
   - Phone Number ID
   - Access Token
   - Webhook Verify Token
   - Webhook Secret
4. Token is encrypted by `encrypt_access_token()`.

Embedded Signup:

1. Ensure env vars:
   - `META_APP_ID`
   - `META_APP_SECRET`
   - `META_EMBEDDED_SIGNUP_REDIRECT_URI`
   - `MESSAGING_TOKEN_ENCRYPTION_KEY`
2. Click "Start Meta Signup" from `/admin/client-audit`.
3. Select business/WABA/phone number if multiple assets exist.
4. Confirm account appears in `messaging_accounts`.

Webhook registration:

```text
https://<domain>/webhooks/meta/<franchise_slug>/<branch_slug>/<inbound_webhook_token>
```

## Client Verification

Before activation:

1. Login works for superadmin/franchise admin/reception.
2. Franchise is active.
3. Branch is active.
4. Public booking link loads.
5. Booking can be created.
6. Booking appears in `/bookings`.
7. Reminder can be prepared/sent.
8. Meta account token status shows configured.
9. Paystack payment link can be generated if billing is used.
10. `/health` returns `{"status":"ok"}`.
11. `/health/db` returns `{"status":"ok"}`.

## Client Activation

Set in `/manage/franchises`:

- `active=true`
- `subscription_status=active`
- `subscription_start`
- `subscription_end`
- plan fields

## Client Offboarding

1. Set franchise `active=false`.
2. Set `subscription_status=cancelled` or `inactive`.
3. Disable users through `/manage/users`.
4. Disable messaging account through `/admin/client-audit`.
5. Preserve records for audit.
6. Do not delete franchise, branch, booking, communication, billing, or webhook rows without a backup.

## Known Onboarding Risks

- Demo accounts are created by `_ensure_demo_access_accounts()`.
- Meta app setup must be completed outside VANTA.
- Paystack requires valid `PAYSTACK_SECRET_KEY` and `PAYSTACK_WEBHOOK_SECRET`.
- `BILLING_EMAIL` or `ADMIN_EMAIL` should be set before generating Paystack links.
