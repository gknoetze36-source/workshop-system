"""Flyer Lady connector UI status -- the six labels the dashboard shows,
and whether each platform may be offered in the publish selector.

    Connected
    Connect
    Reconnect
    Disconnected
    Awaiting approval
    Private beta

This is deliberately a SEPARATE, small module from
flyer_lady/connectors/status.py, not an extension of it.
normalize_status() there was built for a different, already-delivered
task with its own explicit four-value contract (connected,
reconnect_required, disconnected, pending_review) -- notably, it
intentionally collapses "never connected" and "revoked" into one
disconnected bucket, which is correct for that contract and wrong for
this one: this UI needs to tell a workshop that's never connected
Facebook ("Connect") apart from one whose connection was revoked
("Disconnected"). Rather than change status.py's existing, tested
four-value meaning to fit a UI need it wasn't scoped for, this module
reads connector.health()'s raw connection_status itself (the same
underlying call normalize_status() already makes) and does its own,
UI-specific mapping.

Every registered connector is read the exact same way, through
get_connector() -- the connector registry stays the single source of
truth for which platforms exist and what each is capable of; this
module adds no platform-specific branching except where a connector's
own health() already reports something platform-specific
(capability_ready), which every connector may optionally set and which
defaults to True when absent.
"""
from __future__ import annotations

from typing import Any

from .registry import get_connector, registered_platforms

UI_CONNECTED = "Connected"
UI_CONNECT = "Connect"
UI_RECONNECT = "Reconnect"
UI_DISCONNECTED = "Disconnected"
UI_AWAITING_APPROVAL = "Awaiting approval"
UI_PRIVATE_BETA = "Private beta"

UI_STATES = frozenset({
    UI_CONNECTED, UI_CONNECT, UI_RECONNECT, UI_DISCONNECTED, UI_AWAITING_APPROVAL, UI_PRIVATE_BETA,
})

#: Platforms whose connector is explicitly a private/beta
#: implementation -- currently just TikTok
#: (flyer_lady/connectors/tiktok_platform.py's own docstring:
#: "private/beta implementation," every post permanently
#: SELF_ONLY-restricted by TikTok itself until audited). A platform
#: belongs here because of what it IS, not today's connection state,
#: so this is a fixed set, not something read from health() per call.
#:
#: Being private/beta does NOT mean unusable: once genuinely connected
#: and capability-ready, TikTok publishes exactly like any other
#: platform (restricted to SELF_ONLY content, which is the point of
#: the feature, not a block on it) -- Private beta only REPLACES the
#: Connected/Connect labels to keep that restriction visible; it never
#: overrides a genuine problem state (Reconnect, Disconnected) that the
#: workshop actually needs to see and act on.
PRIVATE_BETA_PLATFORMS = frozenset({"tiktok"})


def get_connector_ui_state(session, location_id: int, platform: str) -> dict[str, Any]:
    """The single lookup the Flyer Lady dashboard needs per platform:
    platform, ui_status (one of the six labels above), capabilities,
    and selectable_for_publish -- the exact AND this task requires:
    status == connected AND the required capability is available.
    "Required capability available" means capability_ready is not
    explicitly False; every connector's health() defaults to ready
    when it has no platform-specific gate of its own to report."""
    connector = get_connector(platform)
    health = connector.health(session, location_id)
    capabilities = connector.capabilities()

    raw_status = health.get("connection_status")
    deployment_configured = health.get("deployment_configured", False)
    capability_ready = health.get("capability_ready", True)

    if not deployment_configured:
        ui_status = UI_CONNECT
    elif raw_status == "reconnect_required":
        ui_status = UI_RECONNECT
    elif raw_status == "revoked":
        ui_status = UI_DISCONNECTED
    elif raw_status == "pending_review":
        ui_status = UI_AWAITING_APPROVAL
    elif platform in PRIVATE_BETA_PLATFORMS:
        # Reached only for a non-problem raw_status (connected,
        # not_connected, or unrecognized) -- Private beta stands in for
        # both Connect and Connected here, since the label itself is
        # what tells a workshop this platform works differently, before
        # they even start connecting it as much as after.
        ui_status = UI_PRIVATE_BETA
    elif raw_status == "connected":
        ui_status = UI_CONNECTED
    else:
        # "not_connected" (no connection row at all) or any other
        # unrecognized raw value -- conservatively treated the same as
        # never connected.
        ui_status = UI_CONNECT

    selectable_for_publish = (
        deployment_configured
        and raw_status == "connected"
        and capability_ready
        and bool(capabilities.get("post_types"))
    )

    return {
        "platform": platform,
        "ui_status": ui_status,
        "capabilities": capabilities,
        "selectable_for_publish": selectable_for_publish,
    }


def get_all_connector_ui_states(session, location_id: int) -> list[dict[str, Any]]:
    """Same, for every registered platform -- what the dashboard
    actually renders in one call.

    Each platform is isolated: one connector's health() raising (a
    missing table mid-deploy, a transient error calling out to the
    platform, anything) previously took down this whole list -- and
    with it, the entire Flyer Lady dashboard for every workshop,
    regardless of whether their own connectors were fine. The broken
    platform now falls back to Disconnected/not-selectable -- the
    conservative choice; it must never be reported Connected when its
    own health check couldn't even run -- while every other platform's
    real status still renders."""
    import logging
    logger = logging.getLogger(__name__)

    states = []
    for platform in registered_platforms():
        try:
            states.append(get_connector_ui_state(session, location_id, platform))
        except Exception:
            logger.exception(
                "flyer_lady_connector_health_failed location_id=%s platform=%s",
                location_id, platform,
            )
            states.append({
                "platform": platform,
                "ui_status": UI_DISCONNECTED,
                "capabilities": {},
                "selectable_for_publish": False,
            })
    return states
