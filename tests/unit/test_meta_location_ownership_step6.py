from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.core import Base, Owner, Location, Customer, Conversation
from models.integration_models import MetaBusinessConnection
from integrations.meta.webhook.webhook_location_resolver import resolve_meta_webhook_location
from integrations.meta.webhook.webhook_router import MetaWebhookRouter


def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)()


def test_meta_assets_are_unique_per_location():
    db = session()
    oa, ob = Owner(name="OA"), Owner(name="OB")
    db.add_all([oa, ob]); db.flush()
    a, b = Location(owner_id=oa.id, name="A"), Location(owner_id=ob.id, name="B")
    db.add_all([a, b]); db.flush()
    db.add(MetaBusinessConnection(location_id=a.id, waba_id="waba-a", phone_number_id="phone-a"))
    db.commit()
    assert db.query(MetaBusinessConnection).filter_by(location_id=a.id).one().phone_number_id == "phone-a"
    db.close()


def test_webhook_resolves_to_the_location_by_phone_id():
    db = session()
    oa, ob = Owner(name="OA"), Owner(name="OB")
    db.add_all([oa, ob]); db.flush()
    a = Location(owner_id=oa.id, name="A"); b = Location(owner_id=ob.id, name="B")
    db.add_all([a, b]); db.flush()
    db.add_all([
        MetaBusinessConnection(location_id=a.id, waba_id="waba-a", phone_number_id="phone-a"),
        MetaBusinessConnection(location_id=b.id, waba_id="waba-b", phone_number_id="phone-b"),
    ])
    db.commit()
    payload = {"entry": [{"id": "waba-a", "changes": [{"value": {"metadata": {"phone_number_id": "phone-a"}}}]}]}
    assert resolve_meta_webhook_location(db, payload) == a.id
    db.close()


def test_unknown_webhook_does_not_create_location_owned_data():
    db = session()
    payload = {
        "object": "whatsapp_business_account",
        "entry": [{"id": "unknown-waba", "changes": [{"field": "messages", "value": {
            "metadata": {"phone_number_id": "unknown-phone"},
            "messages": [{"id": "wamid-unknown", "from": "27820000000", "text": {"body": "Hi"}}]
        }}]}]
    }
    result = MetaWebhookRouter(db).dispatch(payload)
    assert result["results"] == []
    assert db.query(Customer).count() == 0
    assert db.query(Conversation).count() == 0
    db.close()
