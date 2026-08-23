import pytest
from cryptography.fernet import Fernet
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.core import Base, Location, Owner
from models.integration_models import MetaBusinessConnection, MetaBusinessVerificationStatus
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.whatsapp.phone_number_service import PhoneNumberService, PhoneRegistrationError


class FakeClient:
    def __init__(self):
        self.calls = []

    def post_with_token(self, token, path, data=None):
        self.calls.append(("POST", token, path, data))
        return {"success": True}

    def get_with_token(self, token, path, params=None):
        self.calls.append(("GET", token, path, params))
        if path.startswith("/waba"):
            return {"name": "Workshop WABA", "timezone_id": "Africa/Johannesburg", "message_template_namespace": "ns"}
        return {"display_phone_number": "+27123456789", "verified_name": "Workshop", "quality_rating": "GREEN"}


def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def service(session):
    location = Location(owner=Owner(), name="Workshop")
    session.add(location)
    session.commit()
    connection = MetaBusinessConnection(location_id=location.id, waba_id="waba", phone_number_id="phone")
    session.add(connection)
    session.commit()
    store = MetaTokenStore(key=Fernet.generate_key())
    store.save_customer_token(session, connection, "customer-token")
    session.commit()
    client = FakeClient()
    return location.id, PhoneNumberService(client=client, token_store=store), client


def test_register_uses_customer_token_and_never_persists_pin():
    s = db(); location_id, svc, client = service(s)
    result = svc.register(s, location_id, "123456")
    assert result.status == "registered"
    assert client.calls[-1][0] == "POST"
    assert client.calls[-1][1] == "customer-token"
    assert client.calls[-1][2] == "/phone/register"
    assert client.calls[-1][3] == {"messaging_product": "whatsapp", "pin": "123456"}
    row = s.query(MetaBusinessVerificationStatus).one()
    assert row.phone_verification_status == "registered"


def test_invalid_pin_rejected_before_api_call():
    s = db(); location_id, svc, client = service(s)
    with pytest.raises(PhoneRegistrationError, match="exactly 6 digits"):
        svc.register(s, location_id, "12345")
    assert client.calls == []


def test_request_and_verify_code_update_status():
    s = db(); location_id, svc, client = service(s)
    assert svc.request_verification_code(s, location_id, code_method="SMS", language="en_US").status == "code_requested"
    assert svc.verify_code(s, location_id, "123456").status == "verified"
    row = s.query(MetaBusinessVerificationStatus).one()
    assert row.phone_verification_status == "verified"


def test_phone_and_waba_info_are_safe():
    s = db(); location_id, svc, client = service(s)
    info = svc.phone_info(s, location_id)
    assert info["display_phone_number"] == "+27123456789"
    assert "access_token" not in info
    waba = svc.waba_info(s, location_id)
    assert waba["name"] == "Workshop WABA"
    assert "access_token" not in waba
