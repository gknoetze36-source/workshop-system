"""Meta Tech Provider onboarding completion.

Every Meta API call is mocked. Nothing here needs a live Meta account, a
network connection or real credentials.

The scenarios map one-to-one onto the required coverage list:

 1. successful complete onboarding          9. webhook subscription failure
 2. WABA verification failure              10. app already subscribed
 3. System User assignment failure         11. template synchronization failure
 4. System User already assigned           12. resume after partial failure
 5. phone registration failure             13. never connected prematurely
 6. credit line retrieval failure          14. tenant/location isolation
 7. credit sharing failure                 15. tokens are never logged
 8. credit sharing already exists
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.core import Base, Location, Owner
from models.integration_models import MetaBusinessConnection, MetaMessageTemplate
from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.business.provider_config import MetaProviderConfig
from integrations.meta.services import onboarding_state as state
from integrations.meta.services.graph_api_client import MetaGraphAPIError
from integrations.meta.services.tech_provider_onboarding_service import (
    OnboardingStepError,
    TechProviderOnboardingService,
    scrub,
)

APP_ID = "1234567890123456"
SYSTEM_USER_ID = "9988776655443322"
BUSINESS_ID = "1010101010101010"
CREDIT_LINE_ID = "5985499441566032"
ALLOCATION_ID = "58501441721238"
FUNDING_ID = "77001100220033"
WABA_ID = "102290129340398"
PHONE_ID = "106540352242922"
CLIENT_BUSINESS_ID = "2020202020202020"
CUSTOMER_TOKEN = "EAAcustomerTOKENvalue000000000000000000000000"
SYSTEM_TOKEN = "EAAsystemUSERtoken0000000000000000000000000000"

ENCRYPTION_KEY = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="


# ---------------------------------------------------------------------------
# Fixtures and fakes
# ---------------------------------------------------------------------------


def auth_config() -> MetaAuthConfig:
    return MetaAuthConfig(
        app_id=APP_ID,
        app_secret="s" * 32,
        graph_api_version="v26.0",
        system_user_token=SYSTEM_TOKEN,
        app_domains=("https://phanta.example",),
        embedded_signup_config_id="123456",
    )


def provider_config(**overrides) -> MetaProviderConfig:
    values = dict(
        business_id=BUSINESS_ID,
        system_user_id=SYSTEM_USER_ID,
        system_user_token=SYSTEM_TOKEN,
        credit_line_id="",
        assigned_tasks=("MANAGE",),
        credit_sharing_enabled=True,
    )
    values.update(overrides)
    return MetaProviderConfig(**values)


class FakeOperations:
    """A scriptable stand-in for TechProviderOperations.

    Each attribute holds either a canned response or an exception to raise,
    so a test names exactly the Meta behaviour it is exercising. Every call is
    recorded so idempotency can be asserted on call counts.
    """

    def __init__(self, **overrides):
        self.calls: list[tuple[str, tuple]] = []
        self.identity = {"id": SYSTEM_USER_ID, "name": "VANTA System User"}
        self.waba = {
            "id": WABA_ID,
            "name": "Client Workshop",
            "currency": "USD",
            "owner_business_info": {"id": CLIENT_BUSINESS_ID, "name": "Client Business"},
            "account_review_status": "APPROVED",
        }
        self.assigned_users: list[dict] = []
        self.assign_result = {"success": True}
        self.extended_credits = [{"id": CREDIT_LINE_ID, "legal_entity_name": "VANTA"}]
        self.existing_allocations: list[dict] = []
        self.share_result = {"success": True, "waba_id": WABA_ID, "allocation_config_id": ALLOCATION_ID}
        self.allocation_config = {"id": ALLOCATION_ID, "receiving_credential": {"id": FUNDING_ID}}
        self.primary_funding_id = FUNDING_ID
        self.subscribed_apps: list[dict] = []
        self.phone = {
            "id": PHONE_ID,
            "display_phone_number": "+27 11 555 0100",
            "verified_name": "Client Workshop",
            "platform_type": "NOT_APPLICABLE",
            "status": "PENDING",
        }
        self.templates = [
            {
                "id": "t1", "name": "booking_confirmed", "language": "en",
                "category": "UTILITY", "status": "APPROVED",
                "components": [{"type": "BODY", "text": "Booked"}],
            }
        ]
        self.errors: dict[str, Exception] = {}
        self.__dict__.update(overrides)

    def _record(self, name, *args):
        self.calls.append((name, args))
        if name in self.errors:
            raise self.errors[name]

    def count(self, name: str) -> int:
        return sum(1 for call, _ in self.calls if call == name)

    # provider-scoped
    def get_system_user_identity(self):
        self._record("get_system_user_identity")
        return self.identity

    def list_assigned_users(self, waba_id):
        self._record("list_assigned_users", waba_id)
        return list(self.assigned_users)

    def assign_system_user(self, waba_id, system_user_id, tasks):
        self._record("assign_system_user", waba_id, system_user_id, tuple(tasks))
        self.assigned_users.append({"id": system_user_id, "name": "VANTA", "tasks": list(tasks)})
        return self.assign_result

    def list_extended_credits(self, business_id):
        self._record("list_extended_credits", business_id)
        return list(self.extended_credits)

    def find_allocation_configs(self, credit_line_id, receiving_business_id):
        self._record("find_allocation_configs", credit_line_id, receiving_business_id)
        return list(self.existing_allocations)

    def share_credit_line(self, credit_line_id, waba_id, waba_currency):
        self._record("share_credit_line", credit_line_id, waba_id, waba_currency)
        return self.share_result

    def get_allocation_config(self, allocation_config_id):
        self._record("get_allocation_config", allocation_config_id)
        return self.allocation_config

    # client-scoped
    def get_waba(self, customer_token, waba_id, *, fields=None):
        self._record("get_waba", waba_id)
        return self.waba

    def get_waba_primary_funding_id(self, customer_token, waba_id):
        self._record("get_waba_primary_funding_id", waba_id)
        return self.primary_funding_id

    def subscribe_app_to_waba(self, customer_token, waba_id):
        self._record("subscribe_app_to_waba", waba_id)
        self.subscribed_apps.append({"whatsapp_business_api_data": {"id": APP_ID, "name": "PHANTA"}})
        return {"success": True}

    def list_subscribed_apps(self, customer_token, waba_id):
        self._record("list_subscribed_apps", waba_id)
        return list(self.subscribed_apps)

    def get_phone_number(self, customer_token, phone_number_id, *, fields=None):
        self._record("get_phone_number", phone_number_id)
        return self.phone

    def iter_message_templates(self, customer_token, waba_id, **kwargs):
        self._record("iter_message_templates", waba_id)
        return iter(list(self.templates))


class FakeGraphClient:
    """Only the calls the orchestrator makes directly on the Graph client."""

    def __init__(self, token_valid: bool = True):
        self.token_valid = token_valid
        self.debug_calls = 0

    def debug_customer_token(self, token, *, timeout=15.0):
        self.debug_calls += 1
        return {"data": {"is_valid": self.token_valid, "granular_scopes": []}}


class FakePhoneService:
    """Stands in for the existing PhoneNumberService.

    Reused rather than reimplemented: the orchestrator must call through to
    the project's real registration service, so the test asserts the call
    happened with the PIN it was given.
    """

    def __init__(self, error: Exception | None = None):
        self.registered: list[tuple[int, str]] = []
        self.error = error

    def register(self, session, location_id, pin):
        if self.error:
            raise self.error
        self.registered.append((location_id, pin))
        return None


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def make_location(db, name="Workshop"):
    location = Location(owner=Owner(), name=name)
    db.add(location)
    db.commit()
    return location


def make_connection(db, location, **overrides):
    store = MetaTokenStore(ENCRYPTION_KEY)
    values = dict(
        location_id=location.id,
        business_id=CLIENT_BUSINESS_ID,
        waba_id=WABA_ID,
        phone_number_id=PHONE_ID,
        connection_status=state.STATUS_ONBOARDING,
        onboarding_step=state.STEP_TOKEN_EXCHANGED,
        last_successful_onboarding_step=state.STEP_TOKEN_EXCHANGED,
        encrypted_access_token=store.encrypt(CUSTOMER_TOKEN),
        token_key_version="v1",
    )
    values.update(overrides)
    connection = MetaBusinessConnection(**values)
    db.add(connection)
    db.commit()
    return connection


def make_service(operations=None, provider=None, phone_service=None, graph=None):
    operations = operations or FakeOperations()
    service = TechProviderOnboardingService(
        config=auth_config(),
        provider=provider or provider_config(),
        client=graph or FakeGraphClient(),
        operations=operations,
        token_store=MetaTokenStore(ENCRYPTION_KEY),
        phone_service=phone_service or FakePhoneService(),
    )
    return service, operations


def registered_phone_ops(**overrides):
    """Operations whose phone number is already registered for Cloud API."""
    ops = FakeOperations(**overrides)
    ops.phone = dict(ops.phone, platform_type="CLOUD_API", status="CONNECTED")
    return ops


# ---------------------------------------------------------------------------
# 1. Successful complete onboarding
# ---------------------------------------------------------------------------


def test_full_onboarding_completes_and_connects(db):
    location = make_location(db)
    connection = make_connection(db, location)
    service, ops = make_service(operations=registered_phone_ops())

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.completed is True, result.error
    assert result.error is None
    assert connection.connection_status == state.STATUS_CONNECTED
    assert connection.onboarding_step == state.STEP_COMPLETED
    assert connection.connected_at is not None

    # Every required Meta operation actually ran, with its evidence persisted.
    assert connection.system_user_id == SYSTEM_USER_ID
    assert connection.system_user_assigned_at is not None
    assert connection.phone_registered_at is not None
    assert connection.credit_line_id == CREDIT_LINE_ID
    assert connection.credit_allocation_config_id == ALLOCATION_ID
    assert connection.credit_verified_at is not None
    assert connection.webhook_verified_at is not None
    assert connection.templates_synced_at is not None
    assert connection.owner_business_id == CLIENT_BUSINESS_ID
    assert connection.waba_currency == "USD"

    template = db.query(MetaMessageTemplate).one()
    assert template.location_id == location.id
    assert template.name == "booking_confirmed"
    assert template.waba_id == WABA_ID


# ---------------------------------------------------------------------------
# 2. WABA verification failure
# ---------------------------------------------------------------------------


def test_waba_verification_failure_stops_onboarding(db):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.errors["get_waba"] = MetaGraphAPIError(
        "Unsupported get request", status_code=400, error={"code": 100}
    )
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.completed is False
    assert result.failed_step == state.STEP_WABA_VERIFIED
    assert result.error["retryable"] is False
    assert connection.connection_status == state.STATUS_FAILED
    assert connection.connection_status != state.STATUS_CONNECTED
    assert connection.last_onboarding_error


def test_waba_id_mismatch_is_rejected(db):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.waba = dict(ops.waba, id="999999999999999")
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()

    assert result.failed_step == state.STEP_WABA_VERIFIED
    assert "different WhatsApp Business Account" in result.error["message"]


# ---------------------------------------------------------------------------
# 3. System User assignment failure
# ---------------------------------------------------------------------------


def test_system_user_assignment_failure(db):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.errors["assign_system_user"] = MetaGraphAPIError(
        "Permissions error", status_code=403, error={"code": 200}
    )
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.failed_step == state.STEP_SYSTEM_USER_ASSIGNED
    assert connection.connection_status != state.STATUS_CONNECTED
    assert connection.system_user_assigned_at is None
    # The step before it still persisted, so a resume does not redo it.
    assert connection.last_successful_onboarding_step == state.STEP_WABA_VERIFIED


def test_system_user_verification_failure_when_meta_does_not_confirm(db):
    location = make_location(db)
    connection = make_connection(db, location)

    class NonPersistingOps(FakeOperations):
        def assign_system_user(self, waba_id, system_user_id, tasks):
            # Meta answers success but the assignment never appears.
            self._record("assign_system_user", waba_id, system_user_id, tuple(tasks))
            return {"success": True}

    ops = NonPersistingOps()
    ops.phone = dict(ops.phone, platform_type="CLOUD_API", status="CONNECTED")
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.failed_step == state.STEP_SYSTEM_USER_VERIFIED
    assert result.error["retryable"] is True
    assert connection.connection_status == state.STATUS_ONBOARDING


# ---------------------------------------------------------------------------
# 4. System User already assigned (idempotent)
# ---------------------------------------------------------------------------


def test_system_user_already_assigned_is_idempotent(db):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.assigned_users = [{"id": SYSTEM_USER_ID, "name": "VANTA", "tasks": ["MANAGE"]}]
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()

    assert result.completed is True, result.error
    # No duplicate assignment was attempted.
    assert ops.count("assign_system_user") == 0
    assign_outcome = next(o for o in result.outcomes if o.step == state.STEP_SYSTEM_USER_ASSIGNED)
    assert assign_outcome.action == "already_done"


def test_meta_reporting_already_assigned_is_not_a_failure(db):
    location = make_location(db)
    make_connection(db, location)

    class AlreadyAssignedOps(FakeOperations):
        def assign_system_user(self, waba_id, system_user_id, tasks):
            self._record("assign_system_user", waba_id, system_user_id, tuple(tasks))
            self.assigned_users.append({"id": system_user_id, "tasks": list(tasks)})
            raise MetaGraphAPIError(
                "The user is already assigned to this object", status_code=400, error={"code": 100}
            )

    ops = AlreadyAssignedOps()
    ops.phone = dict(ops.phone, platform_type="CLOUD_API", status="CONNECTED")
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    assert result.completed is True, result.error


# ---------------------------------------------------------------------------
# 5. Phone registration
# ---------------------------------------------------------------------------


def test_phone_registration_without_pin_pauses_and_is_resumable(db):
    location = make_location(db)
    connection = make_connection(db, location)
    service, ops = make_service()  # phone NOT already registered

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.completed is False
    assert result.awaiting_input is True
    assert result.failed_step == state.STEP_PHONE_REGISTERED
    assert connection.onboarding_step == state.STEP_AWAITING_PHONE_PIN
    # Waiting for input is not a failure: the run stays resumable.
    assert connection.connection_status == state.STATUS_ONBOARDING
    assert connection.last_successful_onboarding_step == state.STEP_SYSTEM_USER_VERIFIED


def test_phone_registration_failure(db):
    from integrations.meta.whatsapp.phone_number_service import PhoneRegistrationError

    location = make_location(db)
    connection = make_connection(db, location)
    service, _ = make_service(
        phone_service=FakePhoneService(error=PhoneRegistrationError("PIN must be exactly 6 digits"))
    )

    result = service.run_onboarding(db, location.id, phone_pin="12345")
    db.commit()
    db.refresh(connection)

    assert result.failed_step == state.STEP_PHONE_REGISTERED
    assert connection.connection_status != state.STATUS_CONNECTED
    assert connection.phone_registered_at is None


def test_phone_registration_uses_existing_phone_service_with_pin(db):
    location = make_location(db)
    make_connection(db, location)
    phone_service = FakePhoneService()
    service, _ = make_service(phone_service=phone_service)

    service.run_onboarding(db, location.id, phone_pin="123456")
    db.commit()

    assert phone_service.registered == [(location.id, "123456")]


def test_already_registered_phone_skips_registration(db):
    location = make_location(db)
    make_connection(db, location)
    phone_service = FakePhoneService()
    service, _ = make_service(operations=registered_phone_ops(), phone_service=phone_service)

    result = service.run_onboarding(db, location.id)
    db.commit()

    assert result.completed is True, result.error
    assert phone_service.registered == []


# ---------------------------------------------------------------------------
# 6. Credit line retrieval failure
# ---------------------------------------------------------------------------


def test_credit_line_retrieval_failure_when_no_credit_line_exists(db):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.extended_credits = []
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.failed_step == state.STEP_CREDIT_LINE_RETRIEVED
    assert result.error["retryable"] is False
    assert "no extended credit line" in result.error["message"]
    assert result.error["remediation"]
    assert connection.connection_status == state.STATUS_FAILED


def test_configured_credit_line_must_belong_to_vanta_business(db):
    location = make_location(db)
    make_connection(db, location)
    service, _ = make_service(
        operations=registered_phone_ops(),
        provider=provider_config(credit_line_id="1111111111111111"),
    )

    result = service.run_onboarding(db, location.id)
    db.commit()

    assert result.failed_step == state.STEP_CREDIT_LINE_RETRIEVED
    assert "META_CREDIT_LINE_ID" in result.error["message"]


def test_credit_line_read_failure_is_reported_with_remediation(db):
    location = make_location(db)
    make_connection(db, location)
    ops = registered_phone_ops()
    ops.errors["list_extended_credits"] = MetaGraphAPIError(
        "Permissions error", status_code=403, error={"code": 200}
    )
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()

    assert result.failed_step == state.STEP_CREDIT_LINE_RETRIEVED
    assert "Financial Editor" in result.error["remediation"]


# ---------------------------------------------------------------------------
# 7. Credit sharing failure
# ---------------------------------------------------------------------------


def test_credit_sharing_failure(db):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.errors["share_credit_line"] = MetaGraphAPIError(
        "Invalid parameter", status_code=400, error={"code": 100}
    )
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.failed_step == state.STEP_CREDIT_SHARED
    assert connection.credit_allocation_config_id is None
    assert connection.connection_status != state.STATUS_CONNECTED
    # The retrieved credit line survives for the resume.
    assert connection.credit_line_id == CREDIT_LINE_ID


def test_unsupported_waba_currency_fails_with_actionable_error(db):
    """Meta does not accept ZAR for credit sharing. Say so, don't guess."""
    location = make_location(db)
    make_connection(db, location)
    ops = registered_phone_ops()
    ops.waba = dict(ops.waba, currency="ZAR")
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()

    assert result.failed_step == state.STEP_CREDIT_SHARED
    assert "ZAR" in result.error["message"]
    assert "USD" in result.error["message"]
    assert ops.count("share_credit_line") == 0


def test_credit_verification_mismatch_fails(db):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.primary_funding_id = "a-different-funding-id"
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.failed_step == state.STEP_CREDIT_VERIFIED
    assert result.error["retryable"] is True
    assert connection.credit_verified_at is None
    assert connection.connection_status != state.STATUS_CONNECTED


def test_credit_sharing_disabled_skips_billing_steps(db):
    location = make_location(db)
    connection = make_connection(db, location)
    service, ops = make_service(
        operations=registered_phone_ops(),
        provider=provider_config(credit_sharing_enabled=False),
    )

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.completed is True, result.error
    assert ops.count("share_credit_line") == 0
    assert ops.count("list_extended_credits") == 0
    assert connection.connection_status == state.STATUS_CONNECTED


# ---------------------------------------------------------------------------
# 8. Credit sharing already exists / idempotent retry
# ---------------------------------------------------------------------------


def test_existing_credit_allocation_is_recovered_not_duplicated(db):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.existing_allocations = [
        {"id": ALLOCATION_ID, "receiving_business": {"id": CLIENT_BUSINESS_ID}}
    ]
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.completed is True, result.error
    assert ops.count("share_credit_line") == 0
    assert connection.credit_allocation_config_id == ALLOCATION_ID


def test_persisted_allocation_short_circuits_sharing(db):
    location = make_location(db)
    make_connection(
        db, location,
        credit_line_id=CREDIT_LINE_ID,
        credit_allocation_config_id=ALLOCATION_ID,
        credit_shared_at=datetime.now(timezone.utc),
    )
    service, ops = make_service(operations=registered_phone_ops())

    result = service.run_onboarding(db, location.id)
    db.commit()

    assert result.completed is True, result.error
    assert ops.count("share_credit_line") == 0
    assert ops.count("find_allocation_configs") == 0


# ---------------------------------------------------------------------------
# 9. + 10. Webhook subscription
# ---------------------------------------------------------------------------


def test_webhook_subscription_failure(db):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.errors["subscribe_app_to_waba"] = MetaGraphAPIError(
        "Permissions error", status_code=403, error={"code": 200}
    )
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.failed_step == state.STEP_WEBHOOK_SUBSCRIBED
    assert connection.webhook_verified_at is None
    assert connection.connection_status != state.STATUS_CONNECTED


def test_unverifiable_subscription_blocks_completion(db):
    """A subscription Meta will not confirm must not be called connected."""
    location = make_location(db)
    connection = make_connection(db, location)

    class SilentSubscribeOps(FakeOperations):
        def subscribe_app_to_waba(self, customer_token, waba_id):
            self._record("subscribe_app_to_waba", waba_id)
            return {"success": True}  # but never appears in subscribed_apps

    ops = SilentSubscribeOps()
    ops.phone = dict(ops.phone, platform_type="CLOUD_API", status="CONNECTED")
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.failed_step == state.STEP_WEBHOOK_VERIFIED
    assert connection.connection_status != state.STATUS_CONNECTED


def test_app_already_subscribed_is_idempotent(db):
    location = make_location(db)
    make_connection(db, location)
    ops = registered_phone_ops()
    ops.subscribed_apps = [{"whatsapp_business_api_data": {"id": APP_ID, "name": "PHANTA"}}]
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()

    assert result.completed is True, result.error
    assert ops.count("subscribe_app_to_waba") == 0


# ---------------------------------------------------------------------------
# 11. Template synchronization
# ---------------------------------------------------------------------------


def test_template_sync_failure(db):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.errors["iter_message_templates"] = MetaGraphAPIError(
        "Rate limit", status_code=429, error={"code": 4}
    )
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.failed_step == state.STEP_TEMPLATES_SYNCED
    assert result.error["retryable"] is True
    assert connection.templates_synced_at is None
    assert connection.connection_status != state.STATUS_CONNECTED


def test_template_sync_is_rerunnable_without_duplicating(db):
    location = make_location(db)
    connection = make_connection(db, location)
    service, ops = make_service(operations=registered_phone_ops())

    service.sync_message_templates(db, connection)
    service.sync_message_templates(db, connection)
    db.commit()

    assert db.query(MetaMessageTemplate).count() == 1


def test_waba_with_no_templates_is_a_documented_reason_not_a_failure(db):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.templates = []
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.completed is True, result.error
    outcome = next(o for o in result.outcomes if o.step == state.STEP_TEMPLATES_SYNCED)
    assert outcome.detail["synced"] == 0
    assert outcome.detail["reason"] == "waba_has_no_templates_yet"
    assert connection.templates_synced_at is not None


# ---------------------------------------------------------------------------
# 12. Resume after partial failure
# ---------------------------------------------------------------------------


def test_resume_after_partial_failure_does_not_repeat_completed_steps(db):
    location = make_location(db)
    connection = make_connection(db, location)

    ops = registered_phone_ops()
    ops.errors["share_credit_line"] = MetaGraphAPIError("Temporary", status_code=500)
    service, _ = make_service(operations=ops)

    first = service.run_onboarding(db, location.id)
    db.commit()
    assert first.failed_step == state.STEP_CREDIT_SHARED
    assert first.error["retryable"] is True

    assigned_calls = ops.count("assign_system_user")
    assert assigned_calls == 1

    # Meta recovers; resume from exactly where it stopped.
    ops.errors.pop("share_credit_line")
    second = service.resume_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert second.completed is True, second.error
    assert connection.connection_status == state.STATUS_CONNECTED
    # The System User was not assigned a second time.
    assert ops.count("assign_system_user") == assigned_calls
    already = [o for o in second.outcomes if o.action == "already_done"]
    assert {o.step for o in already} >= {
        state.STEP_WABA_VERIFIED,
        state.STEP_SYSTEM_USER_ASSIGNED,
        state.STEP_SYSTEM_USER_VERIFIED,
        state.STEP_PHONE_REGISTERED,
        state.STEP_CREDIT_LINE_RETRIEVED,
    }


def test_resume_after_pin_pause_completes(db):
    location = make_location(db)
    connection = make_connection(db, location)
    phone_service = FakePhoneService()
    service, ops = make_service(phone_service=phone_service)

    paused = service.run_onboarding(db, location.id)
    db.commit()
    assert paused.awaiting_input is True

    # The PIN arrives; the phone now registers and the run finishes.
    ops.phone = dict(ops.phone, platform_type="CLOUD_API", status="CONNECTED")
    resumed = service.resume_onboarding(db, location.id, phone_pin="654321")
    db.commit()
    db.refresh(connection)

    assert resumed.completed is True, resumed.error
    assert connection.connection_status == state.STATUS_CONNECTED
    assert connection.onboarding_step == state.STEP_COMPLETED


# ---------------------------------------------------------------------------
# 13. Never connected prematurely
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "failing_call, expected_step",
    [
        ("get_waba", state.STEP_WABA_VERIFIED),
        ("assign_system_user", state.STEP_SYSTEM_USER_ASSIGNED),
        ("list_extended_credits", state.STEP_CREDIT_LINE_RETRIEVED),
        ("share_credit_line", state.STEP_CREDIT_SHARED),
        ("get_allocation_config", state.STEP_CREDIT_VERIFIED),
        ("subscribe_app_to_waba", state.STEP_WEBHOOK_SUBSCRIBED),
        ("iter_message_templates", state.STEP_TEMPLATES_SYNCED),
    ],
)
def test_connection_never_becomes_connected_on_any_step_failure(db, failing_call, expected_step):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.errors[failing_call] = MetaGraphAPIError("Boom", status_code=400, error={"code": 100})
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.completed is False
    assert result.failed_step == expected_step
    assert connection.connection_status != state.STATUS_CONNECTED
    assert connection.connected_at is None
    assert connection.onboarding_step != state.STEP_COMPLETED


def test_embedded_signup_alone_does_not_connect(db):
    """The original bug: signup completing was treated as onboarding done."""
    from integrations.meta.services.embedded_signup_service import EmbeddedSignupService

    class FakeExchange:
        def exchange_embedded_signup_code(self, code):
            return {"access_token": CUSTOMER_TOKEN, "token_type": "bearer", "expires_in": 5184000}

    location = make_location(db)
    signup = EmbeddedSignupService(
        auth_config(), FakeExchange(), token_store=MetaTokenStore(ENCRYPTION_KEY)
    )
    launch = signup.begin(db, location.id)
    db.commit()
    signup.complete(
        db, location_id=location.id, state_nonce=launch.state_nonce, code="one-time-code",
        business_id=CLIENT_BUSINESS_ID, waba_id=WABA_ID, phone_number_id=PHONE_ID,
    )
    db.commit()

    connection = db.query(MetaBusinessConnection).one()
    assert connection.connection_status == state.STATUS_ONBOARDING
    assert connection.connection_status != state.STATUS_CONNECTED
    assert connection.onboarding_step == state.STEP_TOKEN_EXCHANGED
    assert connection.connected_at is None
    # The token is still stored, so onboarding can resume without a re-signup.
    assert connection.encrypted_access_token


def test_storing_a_token_does_not_connect(db):
    location = make_location(db)
    connection = make_connection(
        db, location, connection_status=state.STATUS_PENDING, encrypted_access_token=None
    )
    MetaTokenStore(ENCRYPTION_KEY).save_customer_token(db, connection, CUSTOMER_TOKEN)
    db.commit()
    db.refresh(connection)

    assert connection.connection_status == state.STATUS_PENDING


def test_account_update_webhook_does_not_connect(db):
    from integrations.meta.webhook.event_handlers.account_handlers import MetaAccountHandlers

    location = make_location(db)
    connection = make_connection(db, location)
    MetaAccountHandlers(db).handle(
        location_id=location.id,
        value={"waba_id": WABA_ID, "event": "PARTNER_ADDED"},
    )
    db.commit()
    db.refresh(connection)

    assert connection.connection_status == state.STATUS_ONBOARDING
    assert connection.connection_status != state.STATUS_CONNECTED


def test_valid_token_does_not_promote_incomplete_onboarding(db):
    from integrations.meta.services.token_status_service import MetaTokenStatusService

    location = make_location(db)
    connection = make_connection(db, location)
    service = MetaTokenStatusService(
        config=auth_config(), client=FakeGraphClient(), token_store=MetaTokenStore(ENCRYPTION_KEY)
    )
    health = service.check_connection(db, location.id)
    db.commit()
    db.refresh(connection)

    assert connection.connection_status == state.STATUS_ONBOARDING
    assert health.healthy is False
    assert health.status != state.STATUS_CONNECTED


def test_legacy_connection_keeps_working_and_is_not_re_onboarded(db):
    """Existing production connections must not be invalidated."""
    from integrations.meta.services.token_status_service import MetaTokenStatusService

    location = make_location(db)
    connection = make_connection(
        db, location,
        connection_status=state.STATUS_CONNECTED,
        onboarding_step=state.STEP_LEGACY_CONNECTED,
        last_successful_onboarding_step=state.STEP_LEGACY_CONNECTED,
        connected_at=datetime.now(timezone.utc),
    )
    service, ops = make_service(operations=registered_phone_ops())

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert result.completed is True
    assert connection.connection_status == state.STATUS_CONNECTED
    # Nothing was re-run against Meta for a grandfathered connection.
    assert ops.calls == []

    health = MetaTokenStatusService(
        config=auth_config(), client=FakeGraphClient(), token_store=MetaTokenStore(ENCRYPTION_KEY)
    ).check_connection(db, location.id)
    db.commit()
    db.refresh(connection)
    assert connection.connection_status == state.STATUS_CONNECTED
    assert health.healthy is True


def test_status_endpoint_never_reports_partial_onboarding_as_connected(db):
    location = make_location(db)
    make_connection(db, location, onboarding_step=state.STEP_CREDIT_SHARED)
    service, _ = make_service()

    status = service.onboarding_status(db, location.id)
    assert status["connected"] is False
    assert status["label"] != "Connected"
    assert 0 < status["progress_percent"] < 100


# ---------------------------------------------------------------------------
# 14. Tenant / location isolation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("clashing_field", ["waba_id", "phone_number_id"])
def test_database_refuses_to_map_one_meta_asset_to_two_locations(db, clashing_field):
    """First isolation layer: the schema itself makes the duplicate impossible."""
    from sqlalchemy.exc import IntegrityError

    first = make_location(db, "Workshop A")
    second = make_location(db, "Workshop B")
    make_connection(db, first)

    distinct = {"waba_id": "999888777666555", "phone_number_id": "999888777666556"}
    distinct.pop(clashing_field)  # leave the clashing one equal to Workshop A's

    with pytest.raises(IntegrityError):
        make_connection(db, second, **distinct)
    db.rollback()


class _StubSession:
    """Session that reports a foreign connection for the isolation lookup.

    Exercises the service-level guard on its own. In a live database the
    unique constraints above stop this state from being reachable; the guard
    is the second layer, and it must fail closed rather than call Meta.
    """

    def __init__(self, foreign):
        self.foreign = foreign

    def scalar(self, *_args, **_kwargs):
        return self.foreign

    def flush(self):
        pass


@pytest.mark.parametrize("clashing_field", ["waba_id", "phone_number_id"])
def test_service_refuses_a_meta_asset_owned_by_another_location(db, clashing_field):
    """Second isolation layer: the orchestrator refuses before any Meta call."""
    location = make_location(db)
    connection = make_connection(db, location)
    foreign = MetaBusinessConnection(
        location_id=location.id + 999, waba_id=WABA_ID, phone_number_id=PHONE_ID
    )
    service, ops = make_service(operations=registered_phone_ops())

    with pytest.raises(OnboardingStepError, match="different workshop"):
        service.verify_waba(_StubSession(foreign), connection)

    assert ops.calls == []
    assert connection.connection_status != state.STATUS_CONNECTED


def test_template_sync_does_not_touch_another_tenants_templates(db):
    other = make_location(db, "Other Workshop")
    db.add(
        MetaMessageTemplate(
            location_id=other.id, waba_id="other-waba", name="booking_confirmed",
            language="en", category="UTILITY", status="APPROVED",
        )
    )
    db.commit()

    location = make_location(db)
    connection = make_connection(db, location)
    service, _ = make_service(operations=registered_phone_ops())
    service.sync_message_templates(db, connection)
    db.commit()

    untouched = (
        db.query(MetaMessageTemplate).filter_by(location_id=other.id).one()
    )
    assert untouched.waba_id == "other-waba"
    assert db.query(MetaMessageTemplate).filter_by(location_id=location.id).count() == 1


# ---------------------------------------------------------------------------
# 15. Tokens are never logged, persisted or returned
# ---------------------------------------------------------------------------


def test_error_text_is_scrubbed_of_token_material():
    message = f"Something failed with token {CUSTOMER_TOKEN} and {SYSTEM_TOKEN}"
    cleaned = scrub(message, secrets=(CUSTOMER_TOKEN, SYSTEM_TOKEN))
    assert CUSTOMER_TOKEN not in cleaned
    assert SYSTEM_TOKEN not in cleaned
    assert "[redacted]" in cleaned


def test_meta_error_echoing_a_token_is_not_persisted(db):
    location = make_location(db)
    connection = make_connection(db, location)
    ops = registered_phone_ops()
    ops.errors["get_waba"] = MetaGraphAPIError(
        f"Invalid OAuth access token: {CUSTOMER_TOKEN}", status_code=400, error={"code": 190}
    )
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()
    db.refresh(connection)

    assert CUSTOMER_TOKEN not in (connection.last_onboarding_error or "")
    assert CUSTOMER_TOKEN not in result.error["message"]


def test_no_token_material_is_logged_during_a_full_run(db, caplog):
    location = make_location(db)
    make_connection(db, location)
    service, _ = make_service(operations=registered_phone_ops())

    with caplog.at_level(logging.DEBUG):
        result = service.run_onboarding(db, location.id)
    db.commit()

    assert result.completed is True, result.error
    logged = "\n".join(record.getMessage() for record in caplog.records)
    for secret in (CUSTOMER_TOKEN, SYSTEM_TOKEN, "s" * 32):
        assert secret not in logged


def test_diagnostics_and_status_expose_no_secrets(db):
    location = make_location(db)
    make_connection(db, location)
    service, _ = make_service(operations=registered_phone_ops())
    service.run_onboarding(db, location.id)
    db.commit()

    payload = str(service.diagnostics(db, location.id)) + str(
        service.onboarding_status(db, location.id)
    )
    for secret in (CUSTOMER_TOKEN, SYSTEM_TOKEN, "s" * 32):
        assert secret not in payload
    assert service.provider.safe_summary()["system_user_token_present"] is True
    assert SYSTEM_TOKEN not in str(service.provider.safe_summary())


# ---------------------------------------------------------------------------
# Provider configuration validation
# ---------------------------------------------------------------------------


def test_configured_system_user_id_is_verified_against_meta(db):
    """META_SYSTEM_USER_ID is never trusted on its own."""
    location = make_location(db)
    make_connection(db, location)
    service, _ = make_service(
        operations=registered_phone_ops(),
        provider=provider_config(system_user_id="1111111111111111"),
    )

    result = service.run_onboarding(db, location.id)
    db.commit()

    assert result.completed is False
    assert "META_SYSTEM_USER_ID does not match" in result.error["message"]


def test_missing_provider_configuration_fails_before_any_meta_call(db):
    location = make_location(db)
    make_connection(db, location)
    service, ops = make_service(
        operations=registered_phone_ops(),
        provider=provider_config(business_id="", system_user_id=""),
    )

    result = service.run_onboarding(db, location.id)
    db.commit()

    assert result.completed is False
    assert "META_BUSINESS_ID" in result.error["message"]
    assert ops.calls == []


def test_system_user_token_rejected_by_meta_is_actionable(db):
    location = make_location(db)
    make_connection(db, location)
    ops = registered_phone_ops()
    ops.errors["get_system_user_identity"] = MetaGraphAPIError(
        "Invalid OAuth access token", status_code=401, error={"code": 190}
    )
    service, _ = make_service(operations=ops)

    result = service.run_onboarding(db, location.id)
    db.commit()

    assert result.completed is False
    assert "META_SYSTEM_USER_TOKEN" in result.error["remediation"]


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------


def test_step_ordering_matches_the_documented_sequence():
    assert state.ONBOARDING_STEPS[0] == state.STEP_SIGNUP_RECEIVED
    assert state.ONBOARDING_STEPS[-1] == state.STEP_COMPLETED
    assert state.step_reached(state.STEP_CREDIT_SHARED, state.STEP_WABA_VERIFIED)
    assert not state.step_reached(state.STEP_WABA_VERIFIED, state.STEP_CREDIT_SHARED)
    assert not state.step_reached(None, state.STEP_WABA_VERIFIED)
    assert state.is_complete(state.STEP_COMPLETED)
    assert not state.is_complete(state.STEP_FINAL_VERIFICATION)


def test_progress_never_moves_backwards(db):
    location = make_location(db)
    connection = make_connection(db, location, last_successful_onboarding_step=state.STEP_CREDIT_VERIFIED)
    service, _ = make_service()

    service._advance(connection, state.STEP_WABA_VERIFIED)
    assert connection.last_successful_onboarding_step == state.STEP_CREDIT_VERIFIED


def test_missing_connection_is_a_clear_error(db):
    location = make_location(db)
    service, _ = make_service()
    with pytest.raises(OnboardingStepError, match="No Meta WhatsApp connection"):
        service.run_onboarding(db, location.id)
