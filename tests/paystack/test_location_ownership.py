from models.core import Base, Owner, Location
from models.integration_models import Payment
from integrations.paystack.webhooks.webhook_location_resolver import resolve_paystack_location
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

def test_paystack_resolver_prefers_explicit_location_metadata():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        owner_a = Owner(name="A", email="a@example.com", active=True)
        owner_b = Owner(name="B", email="b@example.com", active=True)
        session.add_all([owner_a, owner_b]); session.flush()
        loc_a = Location(owner_id=owner_a.id, name="A", active=True)
        loc_b = Location(owner_id=owner_b.id, name="B", active=True)
        session.add_all([loc_a, loc_b]); session.flush()
        session.add(Payment(location_id=loc_b.id, reference="REF-B", amount=10, currency="ZAR", status="initialized"))
        session.commit()
        assert resolve_paystack_location(session, {"reference":"REF-B", "metadata":{"phanta_location_id":loc_a.id}}) == loc_a.id
        assert resolve_paystack_location(session, {"reference":"REF-B"}) == loc_b.id
