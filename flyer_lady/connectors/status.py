"""Connector status -- the single lookup the Flyer Lady UI/service
needs: platform, status, capabilities.

Exactly four statuses, as required:
    connected
    reconnect_required
    disconnected
    pending_review

This is a normalization layer, not a new database column or a new
signal. Every connector's health() (flyer_lady/connectors/meta_social.py,
google_business.py -- both unchanged by this task) already reports
deployment_configured and the connection's raw connection_status,
itself an existing free-text String(40) column with no enum/CHECK
constraint (see models/integration_models.py). This module only maps
whatever raw values that column already holds onto the four canonical
statuses above -- it reads through the session the caller passes in,
the same RLS-respecting, location-scoped pattern health() itself
already uses (database/set_location_id()), and requires no new query
of its own.

pending_review is included and correctly passed through if a
connection's raw status is ever exactly "pending_review" -- but nothing
in this codebase currently writes that value. There is no existing
Meta/Google App Review tracking in this database to trigger it from;
inventing that tracking would be new platform-review infrastructure,
not "connector status handling", so it is deliberately left as
supported-but-currently-unreachable rather than fabricated.
"""
from __future__ import annotations

from typing import Any

from .registry import get_connector, registered_platforms

CONNECTED = "connected"
RECONNECT_REQUIRED = "reconnect_required"
DISCONNECTED = "disconnected"
PENDING_REVIEW = "pending_review"

VALID_STATUSES = frozenset({CONNECTED, RECONNECT_REQUIRED, DISCONNECTED, PENDING_REVIEW})

# Every raw connection_status value actually written anywhere in this
# codebase today, for MetaSocialConnection and GoogleBusinessConnection
# specifically (Flyer Lady's own connections -- not
# MetaBusinessConnection/WhatsApp's separate onboarding-state model,
# which has its own richer vocabulary and is out of scope here):
#   "connected"          -- set on successful OAuth completion
#                            (routes/flyer_lady.py, routes/google_business.py)
#                            and by GoogleTokenStore.save_refresh_token()
#   "reconnect_required"  -- set by flyer_lady/publish_service.py's except
#                            block on a 401/403 (retry_policy.py), for
#                            any of the three platforms
#   "revoked"             -- set by services/meta_data_deletion.py's
#                            deauthorize, offboarding_service.py's
#                            _disconnect_integrations(), and
#                            GoogleBusinessConnector.revoke()
#   "not_connected"       -- not a stored value; what each connector's
#                            own health() reports when no connection
#                            row exists at all for this location
_RAW_STATUS_MAP: dict[str, str] = {
    "connected": CONNECTED,
    "reconnect_required": RECONNECT_REQUIRED,
    "revoked": DISCONNECTED,
    "not_connected": DISCONNECTED,
    "pending_review": PENDING_REVIEW,
}


def normalize_status(*, deployment_configured: bool, raw_connection_status: str | None) -> str:
    """Maps health()'s two existing signals onto one of the four
    canonical statuses. An unconfigured deployment (missing
    META_FLYER_LADY_*/GOOGLE_* credentials) always means disconnected,
    regardless of whatever a stale connection row might say -- nothing
    can actually work without credentials, connected-looking database
    state notwithstanding. A raw value this module has never seen
    (defensive -- the column has no enum constraint, so nothing stops
    something else from writing an unexpected string) conservatively
    maps to disconnected rather than silently claiming connected."""
    if not deployment_configured:
        return DISCONNECTED
    if raw_connection_status is None:
        return DISCONNECTED
    return _RAW_STATUS_MAP.get(raw_connection_status, DISCONNECTED)


def get_connector_status(session, location_id: int, platform: str) -> dict[str, Any]:
    """The lookup this task asks for: platform, status, capabilities --
    for one registered platform. Calls the existing, unchanged health()
    and capabilities() on that platform's connector; adds no new query
    of its own."""
    connector = get_connector(platform)
    health = connector.health(session, location_id)
    status = normalize_status(
        deployment_configured=health["deployment_configured"],
        raw_connection_status=health.get("connection_status"),
    )
    return {
        "platform": connector.platform_key,
        "status": status,
        "capabilities": connector.capabilities(),
    }


def get_all_connector_statuses(session, location_id: int) -> list[dict[str, Any]]:
    """Same, for every registered platform at once -- what a Flyer Lady
    connections view actually wants: one call, one list, ready to
    render."""
    return [get_connector_status(session, location_id, platform) for platform in registered_platforms()]
