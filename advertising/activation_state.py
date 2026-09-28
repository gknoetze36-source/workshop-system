"""The activation-state machine every platform in the Marketing &
Advertising Platform Engine moves through -- one shared model, not a
per-platform reinvention, exactly as instructed.

Two chains, exactly as specified:

    PLANNED -> IMPLEMENTING -> CONNECTED -> TESTING -> VERIFIED -> ACTIVE
    ACTIVE -> DEGRADED -> REPAIR -> VERIFIED -> ACTIVE

This module holds the states and the transition rules. It does not
decide *when* a real platform has actually earned a transition --
that judgement is recorded in PLATFORM_REGISTRY (registry.py) as a
plain, inspectable fact about each platform, evidenced in this
session's own DISCOVER/AUDIT/PROVE/GATE record, not asserted here.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class ActivationState(str, Enum):
    PLANNED = "planned"
    IMPLEMENTING = "implementing"
    CONNECTED = "connected"
    TESTING = "testing"
    VERIFIED = "verified"
    ACTIVE = "active"
    DEGRADED = "degraded"
    REPAIR = "repair"


#: The forward-build chain and the incident-recovery chain, as two
#: separate directed paths sharing the VERIFIED/ACTIVE states where
#: they rejoin. A platform that has never been ACTIVE cannot enter
#: DEGRADED/REPAIR -- those only make sense for something that was
#: live and broke, matching the ACTIVE -> DEGRADED -> REPAIR -> ...
#: chain being written as its own line, not a branch off PLANNED.
_ALLOWED_TRANSITIONS: dict[ActivationState, frozenset[ActivationState]] = {
    ActivationState.PLANNED: frozenset({ActivationState.IMPLEMENTING}),
    ActivationState.IMPLEMENTING: frozenset({ActivationState.CONNECTED, ActivationState.PLANNED}),
    ActivationState.CONNECTED: frozenset({ActivationState.TESTING, ActivationState.IMPLEMENTING}),
    ActivationState.TESTING: frozenset({ActivationState.VERIFIED, ActivationState.CONNECTED}),
    ActivationState.VERIFIED: frozenset({ActivationState.ACTIVE, ActivationState.TESTING}),
    ActivationState.ACTIVE: frozenset({ActivationState.DEGRADED}),
    ActivationState.DEGRADED: frozenset({ActivationState.REPAIR}),
    ActivationState.REPAIR: frozenset({ActivationState.VERIFIED}),
}


class InvalidActivationTransition(ValueError):
    pass


def validate_transition(current: ActivationState, target: ActivationState) -> None:
    """Raises if the transition skips a required step. Does not
    perform the transition or check any of the gate criteria below --
    only that the state graph itself isn't being violated (e.g.
    PLANNED straight to ACTIVE, or TESTING straight to ACTIVE without
    passing through VERIFIED)."""
    allowed = _ALLOWED_TRANSITIONS.get(current, frozenset())
    if target not in allowed:
        raise InvalidActivationTransition(
            f"{current.value} -> {target.value} is not a valid activation transition "
            f"(allowed from {current.value}: {sorted(a.value for a in allowed)})"
        )


#: The eight gate criteria a platform must clear before ACTIVE is a
#: legitimate transition, exactly as specified. This is a checklist
#: shape, not enforcement logic -- PLATFORM_REGISTRY records each
#: platform's real answer against these, evidenced, not assumed.
ACTIVATION_GATE_CRITERIA = (
    "connection_works",
    "permissions_correct",
    "account_resolution_works",
    "capability_works",
    "errors_handled",
    "tenant_isolation_proven",
    "tests_pass",
    "production_verification_passes",
)


@dataclass(frozen=True)
class GateEvidence:
    """One platform's answer against each ACTIVATION_GATE_CRITERIA
    item -- True/False/None (None = not applicable to this platform,
    e.g. production_verification_passes for a platform with no
    reachable production endpoint to verify against yet), plus a short
    note pointing at the actual evidence rather than asserting the
    verdict bare."""
    connection_works: bool | None = None
    permissions_correct: bool | None = None
    account_resolution_works: bool | None = None
    capability_works: bool | None = None
    errors_handled: bool | None = None
    tenant_isolation_proven: bool | None = None
    tests_pass: bool | None = None
    production_verification_passes: bool | None = None
    notes: dict[str, str] = field(default_factory=dict)

    def clears_gate(self) -> bool:
        """ACTIVE requires every criterion to be True. A None
        (not applicable) does NOT clear the gate on its own -- an
        inapplicable criterion should be a rare, deliberate exception
        (e.g. a platform with genuinely no production endpoint to
        verify against), not a way to skip verification quietly. As
        written, only an explicit True counts."""
        values = (
            self.connection_works, self.permissions_correct, self.account_resolution_works,
            self.capability_works, self.errors_handled, self.tenant_isolation_proven,
            self.tests_pass, self.production_verification_passes,
        )
        return all(v is True for v in values)

    def failing_criteria(self) -> list[str]:
        result = []
        for name in ACTIVATION_GATE_CRITERIA:
            if getattr(self, name) is not True:
                result.append(name)
        return result
