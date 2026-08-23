# PHANTA Step 3 — Owner → Location Onboarding

## Implemented flow

```text
PHANTA
  │
  ▼
Owner creates account
  │
  ▼
Owner authenticated
(owner_id set, location_id = NULL)
  │
  ▼
Create Location
  │
  ├── Location name
  └── Industry
  │
  ▼
Owner → exactly one Location
  │
  ▼
Business/location configuration
  │
  ▼
Industry-specific services
  │
  ▼
WhatsApp / Meta configuration
  │
  ▼
Automation configuration
  │
  ▼
Review
  │
  ▼
Complete onboarding
  │
  ▼
Operational dashboard
```

## Ownership guarantees

- New owner accounts are created without a location so onboarding can create it explicitly.
- `owners.user_id` identifies the owner account.
- `locations.owner_id` identifies the owner's single location.
- The database foundation has `locations.owner_id UNIQUE`, so an owner cannot have a second location.
- The session receives `owner_id` immediately after account creation and receives `location_id` only after location creation.
- Location configuration uses the authenticated session's `location_id`.
- `active_location_required()` verifies that the session's location belongs to the authenticated owner.
- Existing owners with a location are sent directly to business configuration.
- Platform administrators bypass business-location onboarding.

## Industry

The first onboarding selection currently exposes the industries already represented by the implementation:
- workshop
- salon
- barber

No unrelated industry system was introduced.

## Testing

- Python AST validation: 0 syntax errors.
- Owner/location foundation tests: 3 passed.
- Step 3 owner/location provisioning tests: 3 passed.
- Total focused tests: 6 passed.

A pre-existing syntax defect in `database/bootstrap.py` (duplicate `location` argument) was corrected because it prevented the application test suite from importing.
