"""Coexistence: the workshop keeps its WhatsApp Business app and PHANTA mirrors it."""
from models.core import Customer, Message, Task
from models.integration_models import MetaBusinessConnection
from integrations.meta.webhook.webhook_router import MetaWebhookRouter
from integrations.meta.services.tech_provider_onboarding_service import TechProviderOnboardingService
from tests.unit.test_meta_webhook_phase8 import seed_connection, setup_session
from tests.unit.test_meta_tech_provider_onboarding import db  # noqa: F401  (fixture)

BIZ = "27110000000"
CUSTOMER = "27820000000"  # Jane Doe, seeded


def _send(session, field, value):
    value = dict(value, metadata={"phone_number_id": "phone-1", "display_phone_number": BIZ})
    payload = {"object": "whatsapp_business_account",
               "entry": [{"id": "waba-1", "changes": [{"field": field, "value": value}]}]}
    result = MetaWebhookRouter(session).dispatch(payload)
    session.commit()
    return result


def test_app_reply_shows_in_inbox_and_pauses_the_ai():
    session = setup_session(); location_id, customer_id = seed_connection(session)
    echo = {"from": BIZ, "to": CUSTOMER, "id": "wamid-echo", "timestamp": "1760000000",
            "type": "text", "text": {"body": "Sure, bring it Monday"}}
    _send(session, "smb_message_echoes", {"message_echoes": [echo]})
    _send(session, "smb_message_echoes", {"message_echoes": [echo]})  # redelivery

    msgs = session.query(Message).filter_by(whatsapp_message_id="wamid-echo").all()
    assert len(msgs) == 1 and msgs[0].direction == "outbound" and msgs[0].body == "Sure, bring it Monday"
    tasks = session.query(Task).filter_by(type="human_handoff", related_entity=f"customer:{customer_id}").all()
    assert len(tasks) == 1 and tasks[0].status == "open"


def test_history_imports_both_directions_without_ai():
    session = setup_session(); location_id, _ = seed_connection(session)
    history = {"history": [{"metadata": {"phase": 0, "chunk_order": 1, "progress": 100}, "threads": [{
        "id": "27831112222",
        "messages": [
            {"from": "27831112222", "id": "wamid-h1", "timestamp": "1750000000", "type": "text", "text": {"body": "Is my car ready?"}},
            {"from": BIZ, "to": "27831112222", "id": "wamid-h2", "timestamp": "1750000100", "type": "text", "text": {"body": "Yes!"}},
        ]}]}]}
    result = _send(session, "history", history)

    assert {r["event_type"] for r in result["results"]} == {"history_message"}
    h1 = session.query(Message).filter_by(whatsapp_message_id="wamid-h1").one()
    h2 = session.query(Message).filter_by(whatsapp_message_id="wamid-h2").one()
    assert (h1.direction, h2.direction) == ("inbound", "outbound")
    assert h1.conversation_id == h2.conversation_id
    assert h1.created_at.year == 2025
    assert session.query(Task).count() == 0


def test_contacts_name_new_customers_but_never_rename_known_ones():
    session = setup_session(); location_id, customer_id = seed_connection(session)
    _send(session, "smb_app_state_sync", {"state_sync": [
        {"type": "contact", "action": "add", "contact": {"full_name": "Piet Pompies", "phone_number": "27834445555"}},
        {"type": "contact", "action": "add", "contact": {"full_name": "Someone Else", "phone_number": CUSTOMER}},
    ]})
    new = session.query(Customer).filter_by(whatsapp_number="27834445555").one()
    assert (new.first_name, new.last_name) == ("Piet", "Pompies")
    assert session.get(Customer, customer_id).first_name == "Jane"


def test_app_disconnect_marks_reconnect_required():
    session = setup_session(); seed_connection(session)
    _send(session, "account_update", {"event": "ACCOUNT_OFFBOARDED", "waba_info": {"waba_id": "waba-1"}})
    assert session.query(MetaBusinessConnection).one().connection_status == "reconnect_required"


def test_business_app_number_needs_no_pin():
    assert TechProviderOnboardingService._phone_already_registered({"is_on_biz_app": True, "platform_type": "NOT_APPLICABLE"})
    assert not TechProviderOnboardingService._phone_already_registered({"platform_type": "NOT_APPLICABLE"})


def test_business_app_number_connects_without_pin_and_syncs_after_webhooks(db):
    from tests.unit import test_meta_tech_provider_onboarding as t
    location = t.make_location(db)
    t.make_connection(db, location)
    ops = t.FakeOperations(phone={"id": t.PHONE_ID, "platform_type": "NOT_APPLICABLE", "status": "PENDING", "is_on_biz_app": True})
    ops.sync_smb_app_data = lambda token, phone_id, sync_type: ops._record("sync_smb_app_data", sync_type) or {"success": True}
    service, _ = t.make_service(operations=ops)

    result = service.run_onboarding(db, location.id)  # no PIN given

    assert result.completed is True, result.error
    names = [c[0] for c in ops.calls]
    syncs = [c[1][0] for c in ops.calls if c[0] == "sync_smb_app_data"]
    assert syncs == ["smb_app_state_sync", "history"]
    assert names.index("sync_smb_app_data") > names.index("subscribe_app_to_waba")


def test_signup_finish_with_only_a_waba_looks_up_the_number(db):
    from tests.unit import test_meta_tech_provider_onboarding as t
    from integrations.meta.auth.token_store import MetaTokenStore
    from integrations.meta.services.embedded_signup_service import EmbeddedSignupService

    class Client:
        def exchange_embedded_signup_code(self, code):
            return {"access_token": t.CUSTOMER_TOKEN, "expires_in": 60}

        def get_with_token(self, token, path, params=None):
            assert path == f"/{t.WABA_ID}/phone_numbers"
            return {"data": [{"id": t.PHONE_ID}]}

    location = t.make_location(db)
    signup = EmbeddedSignupService(t.auth_config(), Client(), token_store=MetaTokenStore(t.ENCRYPTION_KEY))
    launch = signup.begin(db, location.id)
    result = signup.complete(db, location_id=location.id, state_nonce=launch.state_nonce, code="c", waba_id=t.WABA_ID)
    assert result.phone_number_id == t.PHONE_ID
