#!/usr/bin/env python3
"""Pre-flight simulation of Meta Tech Provider onboarding.

Runs the real orchestrator against a mocked Meta Graph API and an in-memory
database. Nothing here touches Meta, the network, or any real credentials, so
it is safe to run on a laptop before the first live onboarding.

    python scripts/simulate_tech_provider_onboarding.py

It simulates the happy path and an induced failure at every major step, and
prints for each scenario:

    PASS / FAIL      did the scenario behave as expected
    FAILED STEP      which onboarding step stopped the run
    RETRYABLE        is resuming the right response
    FINAL STATUS     the resulting connection_status

The scenario list mirrors the automated suite in
tests/unit/test_meta_tech_provider_onboarding.py; this script exists so the
same behaviour can be eyeballed as a sequence rather than read as assertions.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")

from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

from models.core import Base, Location, Owner  # noqa: E402
from models.integration_models import MetaBusinessConnection  # noqa: E402
from integrations.meta.auth.token_store import MetaTokenStore  # noqa: E402
from integrations.meta.services import onboarding_state as state  # noqa: E402
from integrations.meta.services.graph_api_client import MetaGraphAPIError  # noqa: E402
from integrations.meta.services.tech_provider_onboarding_service import (  # noqa: E402
    TechProviderOnboardingService,
)

from tests.unit.test_meta_tech_provider_onboarding import (  # noqa: E402
    CUSTOMER_TOKEN,
    FakeGraphClient,
    FakePhoneService,
    auth_config,
    provider_config,
    registered_phone_ops,
    PHONE_ID,
    WABA_ID,
    CLIENT_BUSINESS_ID,
    ENCRYPTION_KEY,
)

GREEN, RED, YELLOW, DIM, RESET = "\033[32m", "\033[31m", "\033[33m", "\033[2m", "\033[0m"
if not sys.stdout.isatty():
    GREEN = RED = YELLOW = DIM = RESET = ""


def fresh_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def seed(session):
    location = Location(owner=Owner(), name="Simulation Workshop")
    session.add(location)
    session.commit()
    session.add(
        MetaBusinessConnection(
            location_id=location.id,
            business_id=CLIENT_BUSINESS_ID,
            waba_id=WABA_ID,
            phone_number_id=PHONE_ID,
            connection_status=state.STATUS_ONBOARDING,
            onboarding_step=state.STEP_TOKEN_EXCHANGED,
            last_successful_onboarding_step=state.STEP_TOKEN_EXCHANGED,
            encrypted_access_token=MetaTokenStore(ENCRYPTION_KEY).encrypt(CUSTOMER_TOKEN),
            token_key_version="v1",
        )
    )
    session.commit()
    return location


def build(operations, provider=None, phone_service=None):
    return TechProviderOnboardingService(
        config=auth_config(),
        provider=provider or provider_config(),
        client=FakeGraphClient(),
        operations=operations,
        token_store=MetaTokenStore(ENCRYPTION_KEY),
        phone_service=phone_service or FakePhoneService(),
    )


def graph_error(message, status=400, code=100):
    return MetaGraphAPIError(message, status_code=status, error={"code": code})


# --- scenarios --------------------------------------------------------------
# (name, operations factory, expected failed step or None for success)

SCENARIOS = [
    ("Happy path — full onboarding", lambda: registered_phone_ops(), None),
    (
        "WABA verification fails",
        lambda: _with_error("get_waba", graph_error("Unsupported get request")),
        state.STEP_WABA_VERIFIED,
    ),
    (
        "System User assignment fails",
        lambda: _with_error("assign_system_user", graph_error("Permissions error", 403, 200)),
        state.STEP_SYSTEM_USER_ASSIGNED,
    ),
    (
        "System User already assigned (idempotent)",
        lambda: _preassigned(),
        None,
    ),
    (
        "Credit line retrieval fails — no credit line",
        lambda: _no_credit_line(),
        state.STEP_CREDIT_LINE_RETRIEVED,
    ),
    (
        "Credit sharing fails",
        lambda: _with_error("share_credit_line", graph_error("Invalid parameter")),
        state.STEP_CREDIT_SHARED,
    ),
    (
        "Credit sharing already exists (idempotent)",
        lambda: _existing_allocation(),
        None,
    ),
    (
        "Unsupported WABA currency (ZAR)",
        lambda: _currency("ZAR"),
        state.STEP_CREDIT_SHARED,
    ),
    (
        "Credit allocation cannot be verified",
        lambda: _funding_mismatch(),
        state.STEP_CREDIT_VERIFIED,
    ),
    (
        "Webhook subscription fails",
        lambda: _with_error("subscribe_app_to_waba", graph_error("Permissions error", 403, 200)),
        state.STEP_WEBHOOK_SUBSCRIBED,
    ),
    (
        "App already subscribed (idempotent)",
        lambda: _presubscribed(),
        None,
    ),
    (
        "Template synchronization fails",
        lambda: _with_error("iter_message_templates", graph_error("Rate limit", 429, 4)),
        state.STEP_TEMPLATES_SYNCED,
    ),
]


def _with_error(call, error):
    ops = registered_phone_ops()
    ops.errors[call] = error
    return ops


def _preassigned():
    ops = registered_phone_ops()
    ops.assigned_users = [{"id": provider_config().system_user_id, "tasks": ["MANAGE"]}]
    return ops


def _no_credit_line():
    ops = registered_phone_ops()
    ops.extended_credits = []
    return ops


def _existing_allocation():
    ops = registered_phone_ops()
    ops.existing_allocations = [{"id": "58501441721238", "receiving_business": {"id": CLIENT_BUSINESS_ID}}]
    return ops


def _currency(code):
    ops = registered_phone_ops()
    ops.waba = dict(ops.waba, currency=code)
    return ops


def _funding_mismatch():
    ops = registered_phone_ops()
    ops.primary_funding_id = "unrelated-funding-id"
    return ops


def _presubscribed():
    ops = registered_phone_ops()
    ops.subscribed_apps = [{"whatsapp_business_api_data": {"id": auth_config().app_id}}]
    return ops


# --- runner -----------------------------------------------------------------


def run_scenario(name, ops_factory, expected_failed_step):
    session = fresh_session()
    try:
        location = seed(session)
        service = build(ops_factory())
        result = service.run_onboarding(session, location.id)
        session.commit()

        connection = session.query(MetaBusinessConnection).one()
        passed = result.failed_step == expected_failed_step
        if expected_failed_step is None:
            passed = passed and result.completed and connection.connection_status == state.STATUS_CONNECTED
        else:
            passed = passed and connection.connection_status != state.STATUS_CONNECTED

        verdict = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        retryable = "-" if result.error is None else ("yes" if result.error["retryable"] else "no")
        print(f"  {verdict}  {name}")
        print(f"        FAILED STEP  : {result.failed_step or '-'}")
        print(f"        RETRYABLE    : {retryable}")
        print(f"        FINAL STATUS : {connection.connection_status} / {connection.onboarding_step}")
        if result.error:
            print(f"        {DIM}{result.error['message'][:150]}{RESET}")
        return passed
    finally:
        session.close()


def run_resume_scenario():
    """Fail mid-run, then resume and confirm nothing is repeated."""
    session = fresh_session()
    try:
        location = seed(session)
        ops = registered_phone_ops()
        ops.errors["share_credit_line"] = graph_error("Temporary Meta error", 500, 2)
        service = build(ops)

        first = service.run_onboarding(session, location.id)
        session.commit()
        assignments_before = ops.count("assign_system_user")

        ops.errors.pop("share_credit_line")
        second = service.resume_onboarding(session, location.id)
        session.commit()

        connection = session.query(MetaBusinessConnection).one()
        passed = (
            first.failed_step == state.STEP_CREDIT_SHARED
            and second.completed
            and connection.connection_status == state.STATUS_CONNECTED
            and ops.count("assign_system_user") == assignments_before
        )
        verdict = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"  {verdict}  Resume after partial failure")
        print(f"        FAILED STEP  : {first.failed_step} (first run)")
        print(f"        RETRYABLE    : yes")
        print(f"        FINAL STATUS : {connection.connection_status} / {connection.onboarding_step}")
        print(f"        {DIM}System User assigned {ops.count('assign_system_user')}x across both runs "
              f"(must be {assignments_before}){RESET}")
        return passed
    finally:
        session.close()


def run_pin_pause_scenario():
    session = fresh_session()
    try:
        location = seed(session)
        ops = registered_phone_ops()
        ops.phone = dict(ops.phone, platform_type="NOT_APPLICABLE", status="PENDING")
        service = build(ops)

        paused = service.run_onboarding(session, location.id)
        session.commit()
        connection = session.query(MetaBusinessConnection).one()

        passed = (
            paused.awaiting_input
            and connection.onboarding_step == state.STEP_AWAITING_PHONE_PIN
            and connection.connection_status == state.STATUS_ONBOARDING
        )
        verdict = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
        print(f"  {verdict}  Phone registration pauses for a PIN")
        print(f"        FAILED STEP  : {paused.failed_step}")
        print(f"        RETRYABLE    : yes (awaiting input)")
        print(f"        FINAL STATUS : {connection.connection_status} / {connection.onboarding_step}")
        return passed
    finally:
        session.close()


def main() -> int:
    print()
    print("Meta Tech Provider onboarding — pre-flight simulation (mocked Meta API)")
    print("=" * 74)
    print()
    print("Documented onboarding sequence:")
    for index, step in enumerate(state.ONBOARDING_STEPS, start=1):
        print(f"  {index:>2}. {step}")
    print()
    print("Scenarios")
    print("-" * 74)

    results = [run_scenario(*scenario) for scenario in SCENARIOS]
    results.append(run_pin_pause_scenario())
    results.append(run_resume_scenario())

    print("-" * 74)
    passed = sum(1 for r in results if r)
    total = len(results)
    if passed == total:
        print(f"{GREEN}ALL {total} SCENARIOS PASSED{RESET} — no scenario reached "
              f"connection_status='connected' without completing every step.")
        return 0
    print(f"{RED}{total - passed} of {total} SCENARIOS FAILED{RESET}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
