"""Messages sent after the 24-hour window go out as the approved templates,
with variables in the exact order the approved templates declare."""
from datetime import datetime, timezone

from sqlalchemy import select

from ai.communications.lifecycle import LifecycleCommunicationService
from ai.follow_up.service import DeterministicFollowUpService
from integrations.meta.messaging.messaging_service import MetaMessagingService
from models.core import FollowUp
from models.integration_models import MetaMessageTemplate
from tests.unit.test_booking_reconfirmation import FakeGraph, seed, setup_session


def _approve(session, location_id, name):
    session.add(MetaMessageTemplate(location_id=location_id, name=name, language="en",
                                    category="UTILITY", status="APPROVED"))
    session.commit()


def _params(sent):
    return [p["text"] for p in sent["template"]["components"][0]["parameters"]]


def test_ready_for_collection_uses_template_with_name_vehicle_workshop():
    session = setup_session()
    location, _, _, booking, token_store = seed(session)
    booking.status = "ready_for_collection"
    session.commit()
    _approve(session, location.id, "vehicle_ready_for_collection")
    graph = FakeGraph()
    LifecycleCommunicationService(session, MetaMessagingService(session, graph=graph, token_store=token_store)) \
        .ready_for_collection(booking.id, location.id)
    template = graph.sent[0]["template"]
    assert template["name"] == "vehicle_ready_for_collection"
    assert template["language"]["code"] == "en"
    assert _params(graph.sent[0]) == ["Naledi", "Toyota Hilux", "Reconfirm Workshop"]


def test_missing_template_is_recorded_as_blocked_not_raised():
    session = setup_session()
    location, _, _, booking, token_store = seed(session)
    graph = FakeGraph()
    result = LifecycleCommunicationService(session, MetaMessagingService(session, graph=graph, token_store=token_store)) \
        .booking_missed(booking.id, location.id)
    assert result is None and graph.sent == []
    row = session.scalar(select(FollowUp).where(FollowUp.type == "missed_booking_recovery"))
    assert row.status == "blocked_requires_template"


def test_booking_reminder_uses_template_with_human_date():
    session = setup_session()
    location, customer, _, booking, token_store = seed(session)
    _approve(session, location.id, "booking_reminder_details")
    graph = FakeGraph()
    service = DeterministicFollowUpService(session, MetaMessagingService(session, graph=graph, token_store=token_store))
    service.schedule_booking_reminder(booking)
    service.process_due(location.id, now=datetime(2026, 9, 20, tzinfo=timezone.utc))
    assert graph.sent[0]["template"]["name"] == "booking_reminder_details"
    assert _params(graph.sent[0]) == ["Naledi", "Toyota Hilux", "Reconfirm Workshop", "20 September 2026"]


def test_template_webhook_reads_metas_language_field():
    from integrations.meta.webhook.event_handlers.template_handlers import MetaTemplateHandlers
    session = setup_session()
    location, *_ = seed(session)
    MetaTemplateHandlers(session).handle(location_id=location.id, payload={
        "event": "APPROVED", "message_template_id": 1,
        "message_template_name": "annual_service_due", "message_template_language": "en",
    })
    row = session.scalar(select(MetaMessageTemplate).where(MetaMessageTemplate.name == "annual_service_due"))
    assert row.language == "en" and row.status == "APPROVED"


def test_service_due_uses_template_with_service_type():
    from models.core import Recommendation
    session = setup_session()
    location, _, vehicle, _, token_store = seed(session)
    _approve(session, location.id, "service_due_reminder")
    rec = Recommendation(location_id=location.id, vehicle_id=vehicle.id, service_type="Oil change",
                         due_date=datetime(2026, 9, 1, tzinfo=timezone.utc), status="open")
    session.add(rec); session.commit()
    graph = FakeGraph()
    service = DeterministicFollowUpService(session, MetaMessagingService(session, graph=graph, token_store=token_store))
    now = datetime(2026, 9, 25, tzinfo=timezone.utc)
    service.schedule_service_due(location.id, rec.id, now=now)
    service.process_due(location.id, now=now)
    assert graph.sent[0]["template"]["name"] == "service_due_reminder"
    assert _params(graph.sent[0]) == ["Naledi", "Reconfirm Workshop", "Toyota Hilux", "Oil change"]
