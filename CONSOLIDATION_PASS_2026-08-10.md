# PHANTA Consolidation Pass — 2026-08-10

## Scope
Consolidate the current PHANTA build around the Build_Order:
- canonical Phase 19 workshop/platform dashboards
- canonical Meta Embedded Signup flow
- onboarding -> Embedded Signup wiring
- application blueprint registration
- removal of identified duplicate/legacy files
- Python/Jinja validation

## Changes made

### Application wiring
- Replaced obsolete `dashboard_bp` import with `workshop_dashboard_bp` and `platform_dashboard_bp`.
- Replaced obsolete `vehicle_bp` import with `vehicles_bp`.
- Registered all current route blueprints, including Meta, messaging, bookings, lifecycle, reviews, Service Advisor, webhooks and Paystack.
- Added the authenticated session -> `g.tenant_id` bridge used by the Phase 19/Meta routes.
- Added platform-admin role mapping.
- Added `/` index redirect to the canonical workshop dashboard.
- Moved Flask secret to `FLASK_SECRET_KEY` with a development fallback.
- Added the existing Flask `date` template filter expected by vehicle/customer templates.

### Backend route fixes
- Fixed missing imports in `routes/vehicles.py`.
- Fixed missing imports in `routes/onboarding.py`.
- Removed duplicate `current_user` import in onboarding.
- Canonicalized onboarding WhatsApp setup to Meta Embedded Signup instead of manual access-token entry.
- Canonicalized Settings -> WhatsApp to the same Embedded Signup screen.
- Updated onboarding review/completion to use the canonical Meta business connection table.

### Frontend consolidation
Canonical dashboard templates are now:
- `templates/dashboard/workshop.html`
- `templates/dashboard/platform_admin.html`

The workshop dashboard now includes:
- today's bookings
- vehicles waiting
- overdue vehicles
- booking confirmations
- unanswered-message count
- WhatsApp connection state
- billing state
- lifecycle action partial for booking rows

The Meta connection page is now:
- `templates/connect_whatsapp.html`
- `static/meta/embedded_signup.js`

The JS now loads/initializes the Facebook SDK reliably, starts the server-side signup session, launches `FB.login`, and posts the returned code/session information to the canonical backend callback.

### Onboarding completion
Created the missing:
- `templates/onboarding_automation.html`
- `templates/onboarding_review.html`

Created the missing:
- `templates/settings_business.html`

### Legacy/duplicate removal
Removed:
- duplicate `repositories/catalog_service.py`
- duplicate `repositories/vehicle_service.py`
- duplicate Phase 9 documentation
- legacy dashboard templates (`dashboard.html`, `owner_dashboard.html`, `super_admin_dashboard.html`)
- legacy standalone Meta Embedded Signup template
- legacy onboarding templates not used by the current onboarding route
- manual-token `onboarding_whatsapp.html`
- manual-token `settings_whatsapp.html`
- stale `apply_phase5.py`
- stale `legacy/` directory

## Validation

### Python
- AST parse: PASS — 0 errors
- `compileall`: PASS — exit code 0
- Note: the execution environment emitted an unrelated spreadsheet-runtime startup timeout warning, but Python compilation itself returned success.

### Jinja
- All current HTML templates parse successfully: PASS
- 0 Jinja syntax errors
- Added the Flask `date` filter required by existing customer/vehicle templates.

### Template references
- All `render_template()` references point to existing template files: PASS

### Duplicate scan
- No meaningful exact duplicate source/document files remain.
- Only normal empty package-marker files such as `__init__.py`/`.gitkeep` share identical contents.

### Runtime test limitation
The environment used for this consolidation does not have Flask installed, so a full `import app` / pytest execution could not be completed here. The failure was `ModuleNotFoundError: No module named 'flask'`, not a PHANTA syntax failure.

## Remaining validation gate
Before calling this pass production-ready, run in the project's normal virtual environment:
1. `python -m pytest -q`
2. `python -m compileall -q .`
3. Start Flask and verify `/`, `/dashboard`, `/platform/dashboard`, `/onboarding/whatsapp`, `/settings/whatsapp`, and Meta Embedded Signup.
4. Verify the session `franchise_id` correctly maps to the SQLAlchemy `Tenant.id` in the deployed database.
5. Exercise the Meta callback and webhook flow against Meta test/staging credentials.
