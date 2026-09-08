"""Meta Tech Provider onboarding state machine.

Two separate axes are deliberately kept apart:

``connection_status``
    The connection lifecycle the rest of PHANTA reads (messaging_provider,
    onboarding_service, dashboards). A connection is only usable for
    messaging when it reaches CONNECTED.

``onboarding_step``
    The fine-grained progress marker for the Tech Provider onboarding run.
    It records exactly where a partial onboarding stopped so it can be
    resumed from there instead of being restarted.

Successful Embedded Signup is *not* a completed onboarding. It only produces
``token_exchanged``.
"""
from __future__ import annotations

# --- connection lifecycle ---------------------------------------------------

STATUS_PENDING = "pending"
STATUS_ONBOARDING = "onboarding"
STATUS_CONNECTED = "connected"
STATUS_EXPIRING_SOON = "expiring_soon"
STATUS_RECONNECT_REQUIRED = "reconnect_required"
STATUS_DISCONNECTED = "disconnected"
STATUS_FAILED = "failed"

CONNECTION_STATUSES = (
    STATUS_PENDING,
    STATUS_ONBOARDING,
    STATUS_CONNECTED,
    STATUS_EXPIRING_SOON,
    STATUS_RECONNECT_REQUIRED,
    STATUS_DISCONNECTED,
    STATUS_FAILED,
)

#: Statuses that mean "this connection may be used to send/receive messages".
USABLE_STATUSES = (STATUS_CONNECTED, STATUS_EXPIRING_SOON)


# --- onboarding progress ----------------------------------------------------

STEP_SIGNUP_RECEIVED = "signup_received"
STEP_TOKEN_EXCHANGED = "token_exchanged"
STEP_WABA_VERIFIED = "waba_verified"
STEP_SYSTEM_USER_ASSIGNED = "system_user_assigned"
STEP_SYSTEM_USER_VERIFIED = "system_user_verified"
STEP_PHONE_REGISTERED = "phone_registered"
STEP_CREDIT_LINE_RETRIEVED = "credit_line_retrieved"
STEP_CREDIT_SHARED = "credit_shared"
STEP_CREDIT_VERIFIED = "credit_verified"
STEP_WEBHOOK_SUBSCRIBED = "webhook_subscribed"
STEP_WEBHOOK_VERIFIED = "webhook_verified"
STEP_TEMPLATES_SYNCED = "templates_synced"
STEP_FINAL_VERIFICATION = "final_verification"
STEP_COMPLETED = "completed"

#: The documented execution order. Index position is the ordering authority;
#: nothing else in the codebase may assume a different sequence.
ONBOARDING_STEPS = (
    STEP_SIGNUP_RECEIVED,
    STEP_TOKEN_EXCHANGED,
    STEP_WABA_VERIFIED,
    STEP_SYSTEM_USER_ASSIGNED,
    STEP_SYSTEM_USER_VERIFIED,
    STEP_PHONE_REGISTERED,
    STEP_CREDIT_LINE_RETRIEVED,
    STEP_CREDIT_SHARED,
    STEP_CREDIT_VERIFIED,
    STEP_WEBHOOK_SUBSCRIBED,
    STEP_WEBHOOK_VERIFIED,
    STEP_TEMPLATES_SYNCED,
    STEP_FINAL_VERIFICATION,
    STEP_COMPLETED,
)

_STEP_INDEX = {step: index for index, step in enumerate(ONBOARDING_STEPS)}

#: Connections that were already CONNECTED before the Tech Provider
#: orchestrator existed. They predate onboarding_step and must not be
#: invalidated, re-onboarded automatically, or reported as broken.
STEP_LEGACY_CONNECTED = "legacy_connected"

#: Onboarding stopped because it needs a 6-digit two-step verification PIN
#: from the workshop before the phone number can be registered. This is a
#: waiting state, not a failure: it is resumable as soon as a PIN arrives.
STEP_AWAITING_PHONE_PIN = "awaiting_phone_pin"

#: Steps that are not positions in the sequence.
NON_SEQUENCE_STEPS = (STEP_LEGACY_CONNECTED, STEP_AWAITING_PHONE_PIN)


def step_index(step: str | None) -> int:
    """Position of ``step`` in the documented sequence, or -1 if unknown."""
    if not step:
        return -1
    return _STEP_INDEX.get(step, -1)


def step_reached(current: str | None, target: str) -> bool:
    """True when ``current`` is at or past ``target`` in the sequence."""
    target_index = step_index(target)
    if target_index < 0:
        return False
    return step_index(current) >= target_index


def is_complete(step: str | None) -> bool:
    return step == STEP_COMPLETED


def is_legacy(step: str | None) -> bool:
    return step == STEP_LEGACY_CONNECTED


def next_step(step: str | None) -> str | None:
    index = step_index(step)
    if index < 0 or index + 1 >= len(ONBOARDING_STEPS):
        return None
    return ONBOARDING_STEPS[index + 1]


# --- client-safe progress presentation --------------------------------------

#: Wording shown to the workshop. Deliberately free of Meta provider detail
#: (system users, credit lines, allocation configs) -- those are VANTA's
#: internal billing arrangement, not the client's business.
STEP_LABELS = {
    STEP_SIGNUP_RECEIVED: "Connecting WhatsApp…",
    STEP_TOKEN_EXCHANGED: "Connecting WhatsApp…",
    STEP_WABA_VERIFIED: "Verifying business account…",
    STEP_SYSTEM_USER_ASSIGNED: "Configuring WhatsApp access…",
    STEP_SYSTEM_USER_VERIFIED: "Configuring WhatsApp access…",
    STEP_AWAITING_PHONE_PIN: "Waiting for your 6-digit WhatsApp PIN…",
    STEP_PHONE_REGISTERED: "Registering phone…",
    STEP_CREDIT_LINE_RETRIEVED: "Configuring billing…",
    STEP_CREDIT_SHARED: "Configuring billing…",
    STEP_CREDIT_VERIFIED: "Configuring billing…",
    STEP_WEBHOOK_SUBSCRIBED: "Activating webhooks…",
    STEP_WEBHOOK_VERIFIED: "Activating webhooks…",
    STEP_TEMPLATES_SYNCED: "Synchronizing templates…",
    STEP_FINAL_VERIFICATION: "Finishing up…",
    STEP_COMPLETED: "Connected",
    STEP_LEGACY_CONNECTED: "Connected",
}


def step_label(step: str | None) -> str:
    if not step:
        return "Not connected"
    return STEP_LABELS.get(step, "Connecting WhatsApp…")


def progress_percent(step: str | None) -> int:
    """Coarse 0-100 progress for the connect UI."""
    if step in (STEP_COMPLETED, STEP_LEGACY_CONNECTED):
        return 100
    index = step_index(step)
    if index < 0:
        # awaiting_phone_pin sits between system_user_verified and
        # phone_registered; report the position it actually stopped at.
        if step == STEP_AWAITING_PHONE_PIN:
            index = _STEP_INDEX[STEP_SYSTEM_USER_VERIFIED]
        else:
            return 0
    return int(round(100.0 * index / (len(ONBOARDING_STEPS) - 1)))
