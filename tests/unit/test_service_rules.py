from datetime import datetime, timezone

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models.core import Base, Location, Customer, Vehicle, Service, ServiceRule, Recommendation, Owner
from ai.recommendations.rule_engine import ServiceRuleEngine


def session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return Session(engine)


def setup_vehicle(s, mileage=62000, year=2020):
    location = Location(owner=Owner(), name="Workshop")
    s.add(location); s.flush()
    customer = Customer(location_id=location.id, first_name="Jane", last_name="Doe", whatsapp_number="+27123456789")
    s.add(customer); s.flush()
    vehicle = Vehicle(location_id=location.id, customer_id=customer.id, make="Volkswagen", model="Polo", year=year, mileage=mileage)
    s.add(vehicle); s.flush()
    return location, vehicle


def test_rule_engine_flags_due_mileage_rule():
    s = session()
    location, vehicle = setup_vehicle(s, mileage=62000)
    s.add(ServiceRule(location_id=None, service_type="major_service", interval_km=30000, interval_months=None))
    s.add(Service(location_id=location.id, vehicle_id=vehicle.id, service_type="major_service", performed_at=datetime(2024, 1, 10, tzinfo=timezone.utc), mileage_at_service=30000))
    s.commit()

    result = ServiceRuleEngine(s, location.id).due_services(vehicle.id)
    assert result["due_services"][0]["service_type"] == "major_service"
    assert result["due_services"][0]["status"] == "due"
    assert result["due_services"][0]["due_mileage"] == 60000


def test_rule_engine_uses_time_interval_without_prior_service():
    s = session()
    location, vehicle = setup_vehicle(s, mileage=None, year=datetime.now(timezone.utc).year - 3)
    s.add(ServiceRule(location_id=None, service_type="brake_fluid", interval_km=None, interval_months=24))
    s.commit()

    result = ServiceRuleEngine(s, location.id).due_services(vehicle.id)
    assert result["due_services"]
    assert result["due_services"][0]["service_type"] == "brake_fluid"


def test_specific_location_rule_overrides_generic_rule():
    s = session()
    location, vehicle = setup_vehicle(s, mileage=14000)
    s.add(ServiceRule(location_id=None, service_type="minor_service", interval_km=15000))
    s.add(ServiceRule(location_id=location.id, service_type="minor_service", interval_km=10000))
    s.commit()

    result = ServiceRuleEngine(s, location.id).due_services(vehicle.id)
    assert result["due_services"][0]["due_mileage"] == 10000


def test_persist_due_recommendations_is_idempotent():
    s = session()
    location, vehicle = setup_vehicle(s, mileage=30000)
    s.add(ServiceRule(location_id=None, service_type="major_service", interval_km=30000))
    s.commit()

    engine = ServiceRuleEngine(s, location.id)
    first = engine.persist_due_recommendations(vehicle.id)
    second = engine.persist_due_recommendations(vehicle.id)
    s.commit()

    assert len(first) == 1
    assert len(second) == 1
    rows = s.scalars(select(Recommendation).where(Recommendation.vehicle_id == vehicle.id)).all()
    assert len(rows) == 1


def test_service_advisor_tool_uses_rule_engine():
    from integrations.ai.tools import ServiceAdvisorToolRegistry, ToolContext

    s = session()
    location, vehicle = setup_vehicle(s, mileage=30000)
    s.add(ServiceRule(location_id=None, service_type="major_service", interval_km=30000))
    s.commit()

    registry = ServiceAdvisorToolRegistry(ToolContext(s, location.id, 1, vehicle.customer_id))
    result = registry.execute("get_due_services", {"vehicle_id": vehicle.id})
    assert result["due_services"][0]["status"] == "due"
