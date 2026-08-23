# PHANTA Backend Connection Fixes — 2026-08-11

This pass intentionally fixes the four backend gaps identified by the frontend audit.

## 1. Ready-for-collection duplicate send

- Added an idempotency guard when a matching `ready_for_collection_nudge` follow-up or prior lifecycle audit already exists.
- The lifecycle route now handles the legitimate `None` result and returns `already_notified: true` instead of dereferencing `message.id`.
- A repeat click therefore cannot send another customer message through this path.

## 2. Global customer/vehicle search

- Added `GET /customers/search?q=...`.
- Search is tenant-scoped using the authenticated tenant context.
- Searches real customer fields (name, WhatsApp number, email) and real vehicle fields (make, model, registration, VIN).
- Results contain only fields actually present in the current ORM schema.
- Dashboard search now calls this endpoint rather than pretending the browser can search the entire database locally.

## 3. Per-client SuperAdmin audit read model

This remains intentionally **not implemented in this pass** because the current platform backend does not have a tenant-directory/audit read model with the necessary client-level information. The frontend continues to hide client-specific audit views rather than invent them.

## 4. PHANTA Ghost backend endpoint

- Added `POST /api/ghost/ask`.
- The endpoint is authenticated and tenant-scoped for workshop users.
- Platform-admin requests use the existing aggregate platform dashboard queries.
- Workshop requests use the existing tenant-scoped workshop dashboard queries.
- The endpoint returns live data and documented PHANTA boundaries only.
- It does not invent unavailable client records, health states, currencies, or credentials.
- The browser Ghost now calls this backend endpoint instead of relying solely on browser-side keyword answers.

## Validation

- Python compilation: PASS
- Ghost JavaScript syntax: PASS
- Backend connection tests: 6 passed
- No Meta/OpenAI/Paystack credentials were added to frontend code.
