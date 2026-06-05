# User Creation Setup

## Hierarchy

Runtime hierarchy:

```txt
SUPERADMIN -> Franchise -> Branch -> Users
```

Current active roles:

```txt
super_admin
franchise_admin
reception
```

Planned roles are visible in superadmin reporting only:

```txt
branch_manager
technician
accounts
viewer
```

## Superadmin

Superadmin can:
- Create and update franchises
- Create and move branches
- Create, disable, reassign, and reset users
- View organization reporting
- View client audit and integration status
- Manage Meta messaging accounts

Routes:

```txt
/admin/organization
/admin/client-audit
/manage/franchises
/manage/branches
/manage/users
/manage/credentials
```

## Franchise Admin

Franchise admin can:
- Manage own franchise branches
- Manage own franchise users
- Manage service prices
- View own franchise bookings, customers, reports, reminders, and chatbot inbox

Cannot:
- Create superadmin users
- Move branches between franchises
- View other franchises

## Reception

Reception can:
- Manage bookings for assigned branch
- View branch customers
- Use reminders for branch-scoped bookings

Reception must be linked to a branch.

## Creating A User

1. Sign in as `super_admin` or `franchise_admin`.
2. Open `/manage/users`.
3. Enter username, password, full name, email, role, franchise, and branch.
4. For `reception`, select a branch.
5. Save.

Password hashes are stored in `password_hash`; plaintext password storage is cleared.

## Audit

New user actions are written to `audit_logs`.

Password reset compatibility is preserved through `credential_audit`.

## Last Login

Successful UI and API login updates `users.last_login`.

## Safety Rules

- Do not enable `ALLOW_PUBLIC_DASHBOARD_API` in production.
- Use strong `SECRET_KEY`.
- Reset temporary passwords after onboarding.
- Disable users instead of deleting them.
