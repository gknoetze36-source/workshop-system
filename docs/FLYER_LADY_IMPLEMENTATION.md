# PHANTA Flyer Lady — Implemented Foundation

This build adds Flyer Lady as a separate public-publishing subsystem. It does not modify the existing WhatsApp customer messaging implementation or Service Advisor.

## Implemented

- `flyer_lady/` service, approval, publishing and platform adapters
- `MetaSocialConnection` and short-lived encrypted OAuth session storage
- Facebook Page connection flow
- Facebook Feed photo/text publishing
- Facebook Page Story photo publishing using the Page photo upload -> `photo_stories` flow
- WhatsApp Status prepare-and-handoff asset response
- Instagram feed/story publishing adapter for later enablement
- tracked `/dashboard/flyer-lady/l/<special_id>` booking redirects
- click attribution storage
- tenant-scoped approval and publishing records
- scheduler queue/retry integration
- Alembic migration `0011_flyer_lady_social_publishing`
- Flyer Lady workshop UI at `/dashboard/flyer-lady/ui`
- navigation entry in the main PHANTA sidebar

## Existing infrastructure reused

- `integrations/meta/services/graph_api_client.py`
- `integrations/meta/auth/token_store.py`
- `MetaAuthConfig`
- existing tenant context
- existing `AuditLog`
- existing scheduler
- existing `public_booking_url()`

## Environment variables

Add these to Railway or the runtime environment:

```text
META_SOCIAL_REDIRECT_URI=https://YOUR-PHANTA-DOMAIN/dashboard/flyer-lady/connect/callback
META_SOCIAL_OAUTH_SCOPES=pages_show_list,pages_read_engagement,pages_manage_posts,pages_manage_metadata,business_management,instagram_basic,instagram_content_publish
PHANTA_PUBLIC_BASE_URL=https://YOUR-PHANTA-DOMAIN
FLYER_LADY_UPLOAD_DIR=static/uploads/flyer_lady
```

The existing Meta variables remain required and are reused. No AI variables are required.

## Meta setup

The same Meta App can be used, but it needs the social publishing permissions approved for production use. Facebook Story publishing uses the Page access token and the Page `photo_stories` API. Instagram publishing is implemented as an adapter but should remain disabled until the corresponding Meta review/configuration is complete.

## WhatsApp Status

There is deliberately no WhatsApp Status API call. Flyer Lady prepares the image URL and caption for the workshop to share manually, matching the architecture decision that WhatsApp Status is not officially automatable.

## Important operational note

Uploaded media must be publicly reachable by Meta. The current implementation accepts a validated HTTPS media URL. A future object-storage adapter can replace this without changing the publishing layer.

## Validation performed

- Python compilation across the project succeeded.
- Alembic migration chain was executed against a clean SQLite database through revision `0011`.
- All new Flyer Lady tables were confirmed to exist.
- SQLAlchemy metadata registration for all new tables was confirmed.

The local execution environment did not contain Flask and could not download packages because outbound package installation was unavailable, so the full Flask pytest suite could not be executed in this environment.
