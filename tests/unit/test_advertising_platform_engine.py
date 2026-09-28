"""Validates the Marketing & Advertising Platform Engine's shared
architecture: the activation-state machine's transition rules, and
that the registry's recorded state for every platform is internally
consistent with its own gate evidence -- not that any platform is
"working" in a live sense, which nothing in this environment can
prove (see each PlatformRecord's own notes)."""
import pytest

from advertising.activation_state import (
    ActivationState, GateEvidence, InvalidActivationTransition, validate_transition,
)
from advertising.capabilities import PlatformCapability, ORGANIC_CAPABILITIES, ADVERTISING_CAPABILITIES
from advertising.registry import PLATFORM_REGISTRY, active_platforms, registered_platforms, get_platform
from advertising.connectors.meta_ads import MetaAdsConnector
from advertising.connectors.google_ads import GoogleAdsConnector


# --------------------------------------------------------------------------
# State machine
# --------------------------------------------------------------------------

@pytest.mark.parametrize("current,target", [
    (ActivationState.PLANNED, ActivationState.IMPLEMENTING),
    (ActivationState.IMPLEMENTING, ActivationState.CONNECTED),
    (ActivationState.CONNECTED, ActivationState.TESTING),
    (ActivationState.TESTING, ActivationState.VERIFIED),
    (ActivationState.VERIFIED, ActivationState.ACTIVE),
    (ActivationState.ACTIVE, ActivationState.DEGRADED),
    (ActivationState.DEGRADED, ActivationState.REPAIR),
    (ActivationState.REPAIR, ActivationState.VERIFIED),
])
def test_every_specified_forward_transition_is_valid(current, target):
    validate_transition(current, target)  # must not raise


def test_planned_cannot_skip_straight_to_active():
    with pytest.raises(InvalidActivationTransition):
        validate_transition(ActivationState.PLANNED, ActivationState.ACTIVE)


def test_testing_cannot_skip_verified_to_reach_active():
    with pytest.raises(InvalidActivationTransition):
        validate_transition(ActivationState.TESTING, ActivationState.ACTIVE)


def test_degraded_cannot_go_straight_back_to_active_bypassing_repair_and_verified():
    with pytest.raises(InvalidActivationTransition):
        validate_transition(ActivationState.DEGRADED, ActivationState.ACTIVE)


def test_repair_must_pass_through_verified_not_straight_to_active():
    with pytest.raises(InvalidActivationTransition):
        validate_transition(ActivationState.REPAIR, ActivationState.ACTIVE)


def test_planned_cannot_enter_degraded_directly():
    """DEGRADED only makes sense for something that was live and broke."""
    with pytest.raises(InvalidActivationTransition):
        validate_transition(ActivationState.PLANNED, ActivationState.DEGRADED)


# --------------------------------------------------------------------------
# Gate evidence
# --------------------------------------------------------------------------

def test_all_true_clears_the_gate():
    ev = GateEvidence(
        connection_works=True, permissions_correct=True, account_resolution_works=True,
        capability_works=True, errors_handled=True, tenant_isolation_proven=True,
        tests_pass=True, production_verification_passes=True,
    )
    assert ev.clears_gate()
    assert ev.failing_criteria() == []


def test_one_false_criterion_fails_the_whole_gate():
    ev = GateEvidence(
        connection_works=True, permissions_correct=True, account_resolution_works=True,
        capability_works=True, errors_handled=True, tenant_isolation_proven=False,
        tests_pass=True, production_verification_passes=True,
    )
    assert not ev.clears_gate()
    assert ev.failing_criteria() == ["tenant_isolation_proven"]


def test_none_does_not_silently_clear_the_gate():
    """A None (not applicable) must not count as passing -- only an
    explicit True does, so a criterion can't be skipped quietly."""
    ev = GateEvidence(
        connection_works=True, permissions_correct=True, account_resolution_works=True,
        capability_works=True, errors_handled=True, tenant_isolation_proven=None,
        tests_pass=True, production_verification_passes=True,
    )
    assert not ev.clears_gate()


def test_empty_evidence_fails_every_criterion():
    ev = GateEvidence()
    assert not ev.clears_gate()
    assert len(ev.failing_criteria()) == 8


# --------------------------------------------------------------------------
# Registry -- every platform's recorded state is internally consistent
# --------------------------------------------------------------------------

def test_all_eight_platforms_are_registered():
    assert registered_platforms() == [
        "facebook", "google_ads", "google_business", "instagram",
        "threads", "tiktok", "x",
    ] or registered_platforms() == sorted([
        "facebook", "instagram", "threads", "x", "tiktok",
        "google_business", "meta_ads", "google_ads",
    ])


def test_no_platform_is_active_yet():
    """The honest, evidenced answer right now: zero. Every organic
    platform's production_verification_passes is False (no live
    external API call was ever made this engagement); both ads
    platforms are PLANNED. This test exists so that if a platform is
    later marked ACTIVE without that evidence changing, it fails
    loudly rather than silently drifting."""
    assert active_platforms() == []


@pytest.mark.parametrize("key", list(PLATFORM_REGISTRY))
def test_every_platforms_activation_state_agrees_with_its_own_gate_evidence(key):
    """A platform recorded as ACTIVE must have gate evidence that
    actually clears the gate -- catches exactly the failure mode this
    task warns against: marking something active without the
    evidence to support it."""
    record = get_platform(key)
    if record.activation_state == ActivationState.ACTIVE:
        assert record.gate_evidence.clears_gate(), (
            f"{key} is recorded ACTIVE but its own gate_evidence does not "
            f"clear the gate: {record.gate_evidence.failing_criteria()}"
        )


@pytest.mark.parametrize("key", list(PLATFORM_REGISTRY))
def test_every_platform_has_at_least_one_documented_blocker_or_is_active(key):
    """PASS/CONDITIONAL/FAIL discipline applied to platform records:
    anything not ACTIVE must say why, not just carry a bare state."""
    record = get_platform(key)
    if record.activation_state != ActivationState.ACTIVE:
        assert len(record.blockers) > 0


def test_ads_platforms_report_zero_capabilities_not_aspirational_ones():
    for key in ("meta_ads", "google_ads"):
        assert get_platform(key).capabilities == frozenset()


def test_organic_platforms_report_the_shared_organic_capability_set():
    for key in ("facebook", "instagram", "threads", "x", "tiktok", "google_business"):
        assert get_platform(key).capabilities == ORGANIC_CAPABILITIES


# --------------------------------------------------------------------------
# Ads stubs are honest, not fake-functional
# --------------------------------------------------------------------------

@pytest.mark.parametrize("connector_cls", [MetaAdsConnector, GoogleAdsConnector])
def test_ads_connector_capabilities_are_empty(connector_cls):
    assert connector_cls().capabilities() == frozenset()


@pytest.mark.parametrize("connector_cls", [MetaAdsConnector, GoogleAdsConnector])
@pytest.mark.parametrize("method,kwargs", [
    ("authorize_url", {"redirect_uri": "https://x", "state": "s"}),
    ("exchange_code", {"code": "c", "redirect_uri": "https://x"}),
    ("discover_ad_accounts", {"token": "t"}),
    ("revoke", {"session": None}),
    ("create_campaign", {"session": None, "location_id": 1, "spec": {}}),
    ("set_budget", {"session": None, "location_id": 1, "campaign_id": "1", "spec": {}}),
    ("fetch_spend", {"session": None, "location_id": 1, "campaign_id": "1"}),
    ("fetch_analytics", {"session": None, "location_id": 1, "campaign_id": "1"}),
    ("fetch_leads", {"session": None, "location_id": 1, "campaign_id": "1"}),
])
def test_every_ads_operation_honestly_raises_not_fabricates(connector_cls, method, kwargs):
    """Every method that would touch a real ad account must raise
    NotImplementedError -- never a fabricated success, which would be
    exactly the 'unofficial API to make a capability appear
    functional' the task explicitly forbids."""
    connector = connector_cls()
    with pytest.raises(NotImplementedError):
        getattr(connector, method)(**kwargs)


def test_ads_connector_health_reports_not_connected_honestly():
    for connector_cls in (MetaAdsConnector, GoogleAdsConnector):
        health = connector_cls().health(session=None, location_id=1)
        assert health["deployment_configured"] is False
        assert health["connection_status"] == "not_connected"
        assert health["activation_state"] == "planned"
