# New Client Setup From Start To Finish

Use this checklist when adding a new workshop/client to the VANTA system.

## 1. Prepare Client Details

Collect:

- Business name
- Main contact name
- Contact email
- Contact phone
- Branch/location name
- Branch area/address
- Reception user name
- Reception email
- Reception phone
- Preferred plan
- Public booking enabled or disabled
- WhatsApp Cloud API requirement
- Paystack billing requirement

## 2. Log In As Super Admin

Open the backend app:

```txt
https://your-backend-domain.up.railway.app/login
```

Use the super admin account configured by:

```txt
SUPERADMIN_USERNAME
SUPERADMIN_PASSWORD
```

If demo credentials are enabled, they are controlled by:

```txt
DEMO_SUPERADMIN_USERNAME
DEMO_SUPERADMIN_PASSWORD
```

## 3. Create The Franchise/Business

Go to:

```txt
Manage Franchises
```

Create a franchise with:

- Business name
- Contact email
- Contact phone
- Industry: `workshop`
- Plan code: choose the correct plan
- Active: enabled
- Public base URL: backend Railway URL
- Inbound webhook token: generate a strong secret token

Example webhook token:

```txt
client-name-strong-random-token
```

Do not reuse webhook tokens between clients.

## 4. Provision The Business

After creating the franchise, run the provisioning action if available.

Provisioning prepares:

- Plan limits
- Default service templates
- Automation templates
- Onboarding state
- Business defaults

## 5. Create Branches

Go to:

```txt
Manage Branches
```

Create each branch/location:

- Franchise/business
- Branch name
- Area/location
- Contact email
- Contact phone
- Daily booking capacity
- Public booking enabled or disabled

Public booking URLs use:

```txt
/book/<franchise_slug>/<branch_slug>
```

Example:

```txt
https://your-backend-domain.up.railway.app/book/client-workshop/main-branch
```

## 6. Create Users

Go to:

```txt
Manage Users
```

Create the required users:

- Franchise admin
- Reception user
- Optional branch-specific users

For a reception user:

- Role: `reception`
- Assign the correct franchise
- Assign the correct branch
- Use a temporary password
- Ask them to change the password on first login

## 7. Add Services And Prices

Go to:

```txt
Manage Prices
```

Add services the branch offers, for example:

- Minor service
- Major service
- Brake inspection
- Diagnostics
- Vehicle inspection

Each service should have:

- Service name
- Category
- Price
- Branch or franchise scope
- Active status

## 8. Configure Meta WhatsApp Cloud API

Make sure Railway has the Meta variables:

```txt
META_GRAPH_API_VERSION
META_APP_SECRET
PUBLIC_BASE_URL
```

Create or update provider rows in `messaging_accounts`:

```txt
workshop_id
provider=meta|twilio
channel=whatsapp|sms
account_id
sender_id
access_token
auth_secret
webhook_verify_token
```

For Meta WhatsApp, also mirror the phone number ID in `whatsapp_numbers` for webhook lookup.

Then configure the Meta webhook:

```txt
https://your-backend-domain.up.railway.app/webhooks/meta/whatsapp
```

The verify token must match the active `whatsapp_numbers.webhook_verify_token`. Existing franchise records map to this tenant through `franchises.workshop_id`.

For Twilio, configure:

```txt
https://your-backend-domain.up.railway.app/webhooks/twilio/whatsapp/<franchise_slug>/<branch_slug>/<token>
https://your-backend-domain.up.railway.app/webhooks/twilio/sms/<franchise_slug>/<branch_slug>/<token>
```

## 9. Configure Client Payment/Billing

Make sure Railway has:

```txt
PAYSTACK_SECRET_KEY
PAYSTACK_WEBHOOK_SECRET
PUBLIC_BASE_URL
```

In Paystack, set the webhook:

```txt
https://your-backend-domain.up.railway.app/webhook/paystack
```

Use the admin billing screen to:

- Close billing periods
- Generate payment links
- Mark payments manually if required
- Confirm webhook payments are updating records

## 10. Test Booking Flow

Test as a customer:

1. Open the public booking URL.
2. Create a booking with a phone number in international format.
3. Confirm the booking saves.
4. Confirm the booking appears under `Bookings`.
5. Confirm reception can open and update it.
6. Confirm customer communication logs are recorded.

Phone numbers should use international format:

```txt
+27xxxxxxxxx
```

## 11. Test Reception Flow

Log in as the reception user.

Test:

- Dashboard access
- Reception booking
- Walk-in booking
- Booking status updates
- Customer history
- Reminders
- Branch visibility

Reception users should only see their assigned branch.

## 12. Test Automations

Check:

- Booking confirmation
- Reminder generation
- Missed booking follow-up
- Chatbot inbox
- Failed jobs screen if available

If an automation fails, check Railway logs and failed jobs.

## 13. Connect Frontend Dashboard

Frontend Railway service variables:

```txt
NEXT_PUBLIC_API_URL=https://your-backend-domain.up.railway.app
BACKEND_API_TOKEN=same-token-as-backend
```

Backend Railway service variables:

```txt
FRONTEND_API_TOKEN=same-token-as-frontend
FRONTEND_ORIGIN=https://your-frontend-domain.up.railway.app
```

The frontend reads:

```txt
/api/dashboard
/api/jobs
/api/bookings
/api/customers
/api/vehicles
/api/automations
/api/staff
/api/inventory
/api/reports
/api/billing
/api/settings
```

## 14. Final Go-Live Checklist

- Franchise is active.
- Subscription status is active.
- Branch is active.
- Public booking is enabled if needed.
- Reception user can log in.
- Public booking page works.
- Meta WhatsApp Cloud API sends outbound messages.
- Meta WhatsApp Cloud API inbound webhook works.
- Paystack payment link generation works.
- Paystack webhook works.
- Frontend dashboard loads live data.
- Railway logs show no startup errors.
- Client has login URL and public booking URL.
