# Workshop Platform Upgrade

## What changed

- Reworked the app into a multi-franchise, multi-branch platform.
- Added public booking pages that route customers into a specific branch.
- Added role-based internal access:
  - `reception`
  - `franchise_admin`
  - `super_admin`
- Added management pages for franchises, branches, and users.
- Added reports, customer history, and reminder management screens.
- Added reminder messaging support for Twilio SMS and WhatsApp delivery.
- Updated dependencies to include `requests` for Twilio delivery.

## Current local bootstrap

- Default franchise: `Main Workshop Group`
- Imported branches from the legacy branch user list.
- Imported bookings from `bookings.csv`.
- Imported legacy data currently sits under one starting franchise until you reorganize it in the management screens.
- Legacy and temporary accounts are now forced through a password-change step on first login.
- The bootstrap super admin still uses username `superadmin`, but it is also forced to rotate its password before normal access.

## Demo tenant

- Franchise: `Demo Motor Group`
- Demo branches:
  - `Riverside Demo Branch`
  - `Lakeside Demo Branch`
- Demo users:
  - Franchise admin:
    - username: `demo.franchise`
    - password: `DemoFranchise2026!`
  - Reception:
    - username: `demo.riverside`
    - password: `DemoReception2026!`
  - Reception:
    - username: `demo.lakeside`
    - password: `DemoReception2026!`
- Demo data seeded:
  - 6 sample bookings across the 2 demo branches
  - mixed statuses so you can show current work, completed work, and branch history
- Public demo booking URLs:
  - `/book/demo-motor-group/riverside-demo-branch`
  - `/book/demo-motor-group/lakeside-demo-branch`

## Key routes

- Public:
  - `/`
  - `/book`
  - `/book/<franchise_slug>/<branch_slug>`
- Staff:
  - `/login`
  - `/account/password`
  - `/dashboard`
  - `/bookings`
  - `/add`
  - `/walkin`
  - `/customers`
  - `/reports`
  - `/reminders`
- Management:
  - `/manage/franchises`
  - `/manage/branches`
  - `/manage/users`

## Messaging variables

- Twilio:
  - `TWILIO_ACCOUNT_SID`
  - `TWILIO_AUTH_TOKEN`
  - `TWILIO_SMS_FROM`
  - `TWILIO_WHATSAPP_FROM`
- Optional AI fallback:
  - `OPENAI_API_KEY`

## Client onboarding model

- SMTP/email delivery is no longer part of the platform runtime.
- Super admin manages client messaging setup through Twilio.
- Recommended live setup:
  - register each client using their own work phone number as a Twilio WhatsApp sender
  - use SMS through Twilio only when needed as fallback
- Important:
  - the app does not create Twilio accounts automatically
  - the super admin still needs to complete Twilio and Meta onboarding for each client number
  - non-Twilio work numbers used for WhatsApp must be able to receive SMS or voice verification during Twilio setup

## Verified

- Public pages render.
- Public booking submission works.
- Reception login works.
- Franchise admin login works.
- Super admin login works.
- Dashboard, bookings, reminders, reports, and customers load for all three roles.
- Legacy and temporary accounts are redirected to `/account/password` until they set a new password.
- Demo franchise admin sees both demo branches.
- Each demo reception user only sees their own branch.
