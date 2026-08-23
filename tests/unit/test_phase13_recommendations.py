from datetime import datetime, timezone
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models.core import Base, Location, Customer, Vehicle, Service, ServiceRule, Recommendation, Owner
from ai.recommendations.rule_engine import ServiceRuleEngine
from integrations.ai.tools import ServiceAdvisorToolRegistry, ToolContext


def db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return Session(engine)


def vehicle_setup(s, mileage=62000):
    location = Location(owner=Owner(), name="Phase13 Workshop")
    s.add(location); s.flush()
    customer = Customer(
        location_id=location.id, first_name="Test", last_name="Customer",
        whatsapp_number="+27110000000",
    )
    s.add(customer); s.flush()
    vehicle = Vehicle(
        location_id=location.id, customer_id=customer.id,
        make="Volkswagen", model="Polo", year=2020, mileage=mileage,
    )
    s.add(vehicle); s.flush()
    return location, customer, vehicle


def test_get_due_services_persists_idempotent_recommendation():
    s = db()
    location, customer, vehicle = vehicle_setup(s, 30000)
    s.add(ServiceRule(location_id=None, service_type="major_service", interval_km=30000))
    s.commit()

    registry = ServiceAdvisorToolRegistry(
        ToolContext(s, location.id, 99, customer.id)
    )
    first = registry.execute("get_due_services", {"vehicle_id": vehicle.id})
    second = registry.execute("get_due_services", {"vehicle_id": vehicle.id})

    rows = s.scalars(select(Recommendation).where(
        Recommendation.location_id == location.id,
        Recommendation.vehicle_id == vehicle.id,
        Recommendation.status == "open",
    )).all()

    assert first["due_services"][0]["status"] == "due"
    assert first["recommendation_ids"] == second["recommendation_ids"]
    assert len(rows) == 1


def test_specific_make_model_rule_beats_generic_rule():
    s = db()
    location, _, vehicle = vehicle_setup(s, 14000)
    s.add_all([
        ServiceRule(location_id=None, service_type="minor_service", interval_km=15000),
        ServiceRule(location_id=None, service_type="minor_service", interval_km=12000, make="Volkswagen"),
        ServiceRule(location_id=None, service_type="minor_service", interval_km=10000, make="Volkswagen", model="Polo"),
    ])
    s.commit()

    result = ServiceRuleEngine(s, location.id).due_services(vehicle.id)
    assert result["due_services"][0]["due_mileage"] == 10000


def test_service_history_sets_next_mileage_from_last_service():
    s = db()
    location, _, vehicle = vehicle_setup(s, 62000)
    s.add(ServiceRule(location_id=None, service_type="major_service", interval_km=30000))
    s.add(Service(
        location_id=location.id, vehicle_id=vehicle.id, service_type="major_service",
        performed_at=datetime(2025, 1, 1, tzinfo=timezone.utc), mileage_at_service=30000,
    ))
    s.commit()

    result = ServiceRuleEngine(s, location.id).due_services(vehicle.id)
    assert result["due_services"][0]["due_mileage"] == 60000
    assert result["due_services"][0]["status"] == "due"
