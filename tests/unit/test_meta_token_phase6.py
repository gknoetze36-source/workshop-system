from datetime import datetime, timedelta, timezone

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.core import Base, Location, Owner
from models.integration_models import MetaBusinessConnection
from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.services.token_status_service import MetaTokenStatusService


class FakeDebugClient:
    def __init__(self, payload=None, error=None):
        self.payload = payload
        self.error = error
        self.last_token = None

    def debug_customer_token(self, token):
        self.last_token = token
        if self.error:
            raise self.error
        return self.payload


def cfg():
    return MetaAuthConfig(
        app_id="1234567890123456",
        app_secret="a" * 32,
        graph_api_version="v26.0",
        system_user_token="system-token",
        app_domains=("https://phanta.example",),
        embedded_signup_config_id="123456",
    )


def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def test_token_store_round_trip():
    store = MetaTokenStore(key=Fernet.generate_key())
    encrypted = store.encrypt("customer-secret")
    assert encrypted != "customer-secret"
    assert store.decrypt(encrypted) == "customer-secret"


def test_token_store_persists_encrypted_value():
    s = db()
    location = Location(owner=Owner(), name="Workshop")
    s.add(location)
    s.commit()
    connection = MetaBusinessConnection(location_id=location.id, waba_id="w1")
    s.add(connection)
    s.commit()
    store = MetaTokenStore(key=Fernet.generate_key())
    store.save_customer_token(s, connection, "customer-secret", expires_at=datetime.now(timezone.utc) + timedelta(days=10))
    s.commit()
    assert connection.encrypted_access_token
    assert connection.encrypted_access_token != "customer-secret"
    assert store.get_customer_token(connection) == "customer-secret"
    assert connection.connection_status == "connected"


def test_valid_token_marks_connection_connected():
    s = db()
    location = Location(owner=Owner(), name="Workshop")
    s.add(location)
    s.commit()
    connection = MetaBusinessConnection(location_id=location.id, waba_id="w1")
    s.add(connection)
    s.commit()
    store = MetaTokenStore(key=Fernet.generate_key())
    store.save_customer_token(s, connection, "customer-secret", expires_at=datetime.now(timezone.utc) + timedelta(days=30))
    client = FakeDebugClient({"data": {"is_valid": True, "expires_at": int((datetime.now(timezone.utc) + timedelta(days=30)).timestamp()), "granular_scopes": [{"scope": "whatsapp_business_messaging"}]}})
    svc = MetaTokenStatusService(cfg(), client, store)
    health = svc.check_connection(s, location.id)
    assert health.healthy is True
    assert health.reconnect_required is False
    assert health.status == "connected"
    assert client.last_token == "customer-secret"


def test_expiring_token_marks_expiring_soon():
    s = db()
    location = Location(owner=Owner(), name="Workshop")
    s.add(location)
    s.commit()
    connection = MetaBusinessConnection(location_id=location.id)
    s.add(connection)
    s.commit()
    store = MetaTokenStore(key=Fernet.generate_key())
    expiry = datetime.now(timezone.utc) + timedelta(days=2)
    store.save_customer_token(s, connection, "customer-secret", expires_at=expiry)
    client = FakeDebugClient({"data": {"is_valid": True, "expires_at": int(expiry.timestamp()), "granular_scopes": []}})
    health = MetaTokenStatusService(cfg(), client, store).check_connection(s, location.id)
    assert health.status == "expiring_soon"
    assert health.healthy is True


def test_invalid_token_requires_reconnect():
    s = db()
    location = Location(owner=Owner(), name="Workshop")
    s.add(location)
    s.commit()
    connection = MetaBusinessConnection(location_id=location.id)
    s.add(connection)
    s.commit()
    store = MetaTokenStore(key=Fernet.generate_key())
    store.save_customer_token(s, connection, "customer-secret")
    client = FakeDebugClient({"data": {"is_valid": False, "granular_scopes": []}})
    health = MetaTokenStatusService(cfg(), client, store).check_connection(s, location.id)
    assert health.status == "reconnect_required"
    assert health.healthy is False
    assert health.reconnect_required is True


def test_missing_token_requires_reconnect():
    s = db()
    location = Location(owner=Owner(), name="Workshop")
    s.add(location)
    s.commit()
    s.add(MetaBusinessConnection(location_id=location.id))
    s.commit()
    store = MetaTokenStore(key=Fernet.generate_key())
    health = MetaTokenStatusService(cfg(), FakeDebugClient(), store).check_connection(s, location.id)
    assert health.status == "reconnect_required"
    assert health.reconnect_required is True

class FakeResponse:
    ok = True
    status_code = 200
    text = ""
    def json(self):
        return {"data": {"is_valid": True}}


def test_debug_token_uses_app_token_server_side(monkeypatch):
    from integrations.meta.services.graph_api_client import GraphApiClient

    class Session:
        def __init__(self):
            self.kwargs = None
        def get(self, *args, **kwargs):
            self.kwargs = kwargs
            return FakeResponse()

    sess = Session()
    client = GraphApiClient(cfg(), sess)
    result = client.debug_customer_token("customer-token")
    assert result["data"]["is_valid"] is True
    assert sess.kwargs["params"]["input_token"] == "customer-token"
    assert sess.kwargs["params"]["access_token"] == cfg().app_id + "|" + cfg().app_secret


def test_token_store_rejects_wrong_encryption_key():
    store = MetaTokenStore(key=Fernet.generate_key())
    encrypted = store.encrypt("customer-secret")
    other_store = MetaTokenStore(key=Fernet.generate_key())
    with pytest.raises(ValueError, match="Unable to decrypt"):
        other_store.decrypt(encrypted)


def test_expired_token_requires_reconnect():
    s = db()
    location = Location(owner=Owner(), name="Workshop")
    s.add(location)
    s.commit()
    connection = MetaBusinessConnection(location_id=location.id)
    s.add(connection)
    s.commit()
    store = MetaTokenStore(key=Fernet.generate_key())
    expiry = datetime.now(timezone.utc) - timedelta(minutes=1)
    store.save_customer_token(s, connection, "customer-secret", expires_at=expiry)
    client = FakeDebugClient({"data": {"is_valid": True, "expires_at": int(expiry.timestamp()), "granular_scopes": []}})
    health = MetaTokenStatusService(cfg(), client, store).check_connection(s, location.id)
    assert health.status == "reconnect_required"
    assert health.reconnect_required is True
    assert health.healthy is False


def test_monitor_location_uses_same_health_state_machine():
    s = db()
    location = Location(owner=Owner(), name="Workshop")
    s.add(location)
    s.commit()
    connection = MetaBusinessConnection(location_id=location.id)
    s.add(connection)
    s.commit()
    store = MetaTokenStore(key=Fernet.generate_key())
    expiry = datetime.now(timezone.utc) + timedelta(days=30)
    store.save_customer_token(s, connection, "customer-secret", expires_at=expiry)
    client = FakeDebugClient({"data": {"is_valid": True, "expires_at": int(expiry.timestamp()), "granular_scopes": []}})
    health = MetaTokenStatusService(cfg(), client, store).monitor_location(s, location.id)
    assert health.status == "connected"
    assert connection.last_health_check_at is not None
