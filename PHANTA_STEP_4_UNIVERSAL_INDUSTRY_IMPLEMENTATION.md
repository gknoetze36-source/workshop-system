# PHANTA Step 4 — Universal Platform / Industry Separation

## Implemented architecture

```text
PHANTA
└── Owner
    └── Location (exactly one per Owner)
        ├── Universal platform
        │   ├── Authentication / session
        │   ├── Customers
        │   ├── Vehicles (when applicable)
        │   ├── Bookings
        │   ├── WhatsApp / Meta integration
        │   ├── Automations engine
        │   ├── Paystack billing / payments
        │   ├── AI infrastructure
        │   ├── Notifications
        │   └── Audit / logging
        │
        └── Selected industry
            ├── Workshop
            │   ├── Vehicle subject
            │   ├── Service Advisor
            │   ├── Service rules / intervals
            │   └── Workshop automation templates
            ├── Salon
            │   └── Salon service / booking defaults
            └── Barber
                └── Barber service / booking defaults
```

## Scope migration

The runtime application now uses `location_id` as the sole business scope. The
previous `tenant_id`, `franchise_id`, and `branch_id` identifiers are not used
by active application modules. PostgreSQL RLS and booking overlap constraints
are switched to `location_id` by Alembic revision `0017_location_scope`.

Historical franchise migration code is isolated in
`database/legacy_franchise_migration.py` and is not imported during normal
application startup.

## Industry separation

`services/industry/` owns industry-specific defaults and workflow capabilities.
The onboarding flow reads the selected Location industry and:

1. creates the Location;
2. stores the selected industry on the Location;
3. loads defaults from that industry module;
4. shows only automation templates belonging to that industry;
5. creates automation rules scoped to that Location.

Universal services do not decide workshop/salon behavior themselves.

## Important boundary

The integration transport layer remains universal. Paystack and Meta ownership
is resolved by `location_id`, while the Location's selected industry controls
which industry workflows are enabled.
