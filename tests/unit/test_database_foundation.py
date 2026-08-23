import pytest
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models.core import (
    Base, Location, Customer, Vehicle, Booking, Quote, QuoteLineItem,
    Approval, Conversation, Message, AuditLog, ToolExecution, Owner
)


def make_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return Session(engine)


def test_core_relationships_and_location_scope():
    session = make_session()

    location_a = Location(owner=Owner(), name="Workshop A")
    location_b = Location(owner=Owner(), name="Workshop B")
    session.add_all([location_a, location_b])
    session.flush()

    customer = Customer(
        location_id=location_a.id,
        first_name="George",
        last_name="Test",
        whatsapp_number="+27123456789",
    )
    session.add(customer)
    session.flush()

    vehicle = Vehicle(
        location_id=location_a.id,
        customer_id=customer.id,
        make="Toyota",
        model="Hilux",
        year=2020,
        mileage=60000,
    )
    session.add(vehicle)
    session.flush()

    booking = Booking(
        location_id=location_a.id,
        customer_id=customer.id,
        vehicle_id=vehicle.id,
        start_time=datetime.now(timezone.utc),
        end_time=datetime.now(timezone.utc) + timedelta(hours=1),
        service_type="Service",
    )
    session.add(booking)
    session.commit()

    assert session.scalar(select(Customer).where(Customer.location_id == location_a.id)) is not None
    assert session.scalar(select(Customer).where(Customer.location_id == location_b.id)) is None
    assert booking.vehicle_id == vehicle.id


def test_quote_line_item_and_append_only_approval_record():
    session = make_session()
    location = Location(owner=Owner(), name="Workshop")
    customer = Customer(
        location_id=1, first_name="A", last_name="B", whatsapp_number="+27000000000"
    )
    session.add(location)
    session.flush()
    customer.location_id = location.id
    session.add(customer)
    session.flush()

    quote = Quote(location_id=location.id, customer_id=customer.id, total_amount=Decimal("1000.00"))
    session.add(quote)
    session.flush()

    item = QuoteLineItem(
        location_id=location.id,
        quote_id=quote.id,
        description="Brake inspection",
        price=Decimal("1000.00"),
        status="ai_suggested",
    )
    session.add(item)
    session.flush()

    approval = Approval(
        location_id=location.id,
        quote_line_item_id=item.id,
        decision="approved",
        decided_by="customer",
        raw_message="Yes, please proceed",
        channel="whatsapp",
    )
    session.add(approval)
    session.commit()

    assert approval.id is not None
    assert len(item.approvals) == 1


def test_message_and_tool_execution_are_traceable():
    session = make_session()
    location = Location(owner=Owner(), name="Workshop")
    session.add(location)
    session.flush()

    customer = Customer(
        location_id=location.id,
        first_name="A",
        last_name="B",
        whatsapp_number="+27111111111",
    )
    session.add(customer)
    session.flush()

    conversation = Conversation(
        location_id=location.id,
        customer_id=customer.id,
        channel="whatsapp",
    )
    session.add(conversation)
    session.flush()

    message = Message(
        location_id=location.id,
        conversation_id=conversation.id,
        direction="in",
        channel="whatsapp",
        body="I need a service",
        whatsapp_message_id="wamid.test.1",
    )
    tool = ToolExecution(
        location_id=location.id,
        conversation_id=conversation.id,
        tool_name="capture_customer_context",
        arguments={"service": "service"},
        result={"saved": True},
        success=True,
        latency_ms=12,
    )
    session.add_all([message, tool])
    session.commit()

    assert conversation.messages[0].body == "I need a service"
    assert conversation.tool_executions[0].success is True


def test_audit_log_records_before_after():
    session = make_session()
    location = Location(owner=Owner(), name="Workshop")
    session.add(location)
    session.flush()

    log = AuditLog(
        location_id=location.id,
        actor="staff",
        action="booking.status_changed",
        entity_type="booking",
        entity_id="123",
        before={"status": "pending"},
        after={"status": "confirmed"},
    )
    session.add(log)
    session.commit()

    assert log.before["status"] == "pending"
    assert log.after["status"] == "confirmed"


def test_location_owned_tables_have_explicit_location_id():
    from models import integration_models  # noqa: F401
    expected = {
        "customers", "vehicles", "bookings", "service_records", "conversations",
        "messages", "recommendations", "quotes", "quote_line_items",
        "approvals", "follow_ups", "tasks", "audit_logs",
        "conversation_summaries", "tool_executions",
        "meta_business_connections", "meta_business_verification_status",
        "meta_permissions_grants", "meta_webhook_events", "meta_audit_logs",
        "payment_customers", "payments", "subscriptions", "invoices", "refunds",
        "paystack_webhook_events", "ai_usage_log",
    }
    for table_name in expected:
        table = Base.metadata.tables[table_name]
        assert "location_id" in table.c, table_name


def test_booking_time_order_constraint_is_present():
    table = Base.metadata.tables["bookings"]
    assert any(c.name == "ck_booking_time_order" for c in table.constraints)


def test_location_guard_rejects_cross_location_related_records():
    from repositories.location_guard import LocationGuard, LocationIntegrityError
    session = make_session()
    a, b = Location(owner=Owner(), name="A"), Location(owner=Owner(), name="B")
    session.add_all([a, b]); session.flush()
    customer = Customer(location_id=a.id, first_name="A", last_name="A", whatsapp_number="+27100000001")
    session.add(customer); session.flush()
    with pytest.raises(LocationIntegrityError):
        LocationGuard(session).customer(b.id, customer.id)


def test_data_correction_and_soft_deletion_are_audited():
    from services.data_lifecycle import DataLifecycleService
    session = make_session()
    location = Location(owner=Owner(), name="Workshop")
    session.add(location); session.flush()
    customer = Customer(location_id=location.id, first_name="Old", last_name="Name", whatsapp_number="+27100000002", email="old@example.com")
    session.add(customer); session.flush()
    from repositories.audit_repo import AuditLogRepository
    service = DataLifecycleService(session, AuditLogRepository(session))
    service.correct_customer(location.id, customer.id, "staff", email="new@example.com")
    service.soft_delete_customer(location.id, customer.id, "staff")
    session.commit()
    assert customer.deleted_at is not None
    actions = [x.action for x in session.scalars(select(AuditLog).where(AuditLog.location_id == location.id)).all()]
    assert actions == ["customer.corrected", "customer.deleted"]
