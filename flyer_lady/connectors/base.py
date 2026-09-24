"""A small, uniform interface over Flyer Lady's existing platform
integrations -- Facebook, Instagram, Google Business Profile.

This is NOT a new way to talk to any platform. Every method below is a
thin wrapper that calls straight into code that already exists and is
already exercised in production: MetaSocialGraphClient, GraphApiClient,
GoogleBusinessApiClient, FlyerLadyPublishService, and
services/meta_data_deletion.py. Nothing here reimplements an HTTP call,
a token exchange, or a publish step differently from how it already
happens. Where a genuinely reusable existing function exists, the
connector calls it directly; where none exists yet, the method says so
honestly (NotImplementedError with a clear reason) rather than
inventing behavior to fill the interface out.

Why this exists at all, given nothing new is being built: today,
routes/flyer_lady.py and routes/google_business.py each hardcode their
own platform's OAuth/discovery/publish calls inline, and
flyer_lady/publish_service.py's publish_post() dispatches by a
hardcoded if/elif chain on post.platform. That works fine for three
platforms. This interface exists so a later platform (Threads, X,
TikTok -- explicitly NOT added yet) can be registered without every
call site needing its own new hardcoded branch -- but that migration is
future work. This task only introduces the interface and registers the
three platforms VANTA already has, calling their existing
implementations exactly as they already behave.

Scope, stated once: no new platforms, no new OAuth, no new publishers,
no behavior change. Every existing route continues to work exactly as
it does today; nothing here is wired into them yet.
"""
from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class FlyerLadyConnector(Protocol):
    """What a Flyer Lady platform connector must provide. Every method's
    real docstring lives on the concrete implementation (meta_social.py,
    google_business.py), since what each one actually does -- and which
    existing VANTA function it calls -- differs per platform; this
    Protocol only fixes the shape."""

    platform_key: str

    def authorize_url(self, *, redirect_uri: str, state: str) -> str:
        """Build the OAuth authorization dialog URL for this platform.
        Deliberately takes `state` as a parameter rather than generating
        or storing it: CSRF state and session management are a web-flow
        concern the caller (a Flask route) already owns and continues to
        own -- this method only builds a URL string, the same one the
        existing route already builds."""
        ...

    def exchange_code(self, *, code: str, redirect_uri: str) -> dict[str, Any]:
        """Exchange an OAuth authorization code for a token, via the
        exact same platform client/endpoint the existing route already
        uses. Returns whatever that existing call already returns."""
        ...

    def discover_accounts(self, *, token: str) -> list[dict[str, Any]]:
        """List the accounts/pages/locations this token can act on."""
        ...

    def refresh(self, *, refresh_token: str) -> str:
        """Return a fresh access token. Raises NotImplementedError for a
        platform whose tokens don't work this way (see meta_social.py)."""
        ...

    def revoke(self, session, **kwargs) -> dict[str, Any]:
        """Revoke a connection. Signature varies slightly by platform
        (Meta's existing revoke is keyed by meta_user_id, not
        location_id) -- see each connector's own docstring."""
        ...

    def health(self, session, location_id: int) -> dict[str, Any]:
        """Report whether this platform is deployment-configured and
        whether this location's own connection is healthy."""
        ...

    def capabilities(self) -> dict[str, Any]:
        """What this platform can actually do today -- the real,
        existing post-type strings publish_service.py already
        dispatches on, not an aspirational list."""
        ...

    def publish_step(self, session, location_id: int, post) -> Any:
        """Publish one SpecialPost. Calls
        FlyerLadyPublishService.publish_post() directly -- the same
        method every existing caller (the scheduler, publish_now)
        already uses, and which already dispatches correctly by
        post.platform on its own."""
        ...

    def fetch_metrics(self, session, location_id: int) -> Any:
        """Not implemented for any platform yet -- there is no existing
        VANTA code that fetches post/page metrics for Flyer Lady today,
        so this raises NotImplementedError rather than fabricate a
        response. Kept in the interface because the requirement lists
        it; see each connector's docstring for the honest reason."""
        ...
