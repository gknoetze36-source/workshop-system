"""PROVE -- release blocker #1 from GATE: escalate_to_human() created a
Task with no consumer anywhere in the application. This closes the
loop: WorkshopDashboardQueries.human_handoffs() surfaces it, and
resolving it (the same action the dashboard's new button calls)
actually clears it -- scoped to the correct location, and never
resolvable across a tenant boundary.
"""
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from models.core import Base, Location, Owner, Task
from repositories.task_repo import TaskRepository
from ai.dashboard.queries import WorkshopDashboardQueries


def _seed_two_locations():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    session = Session(engine)
    a = Location(owner=Owner(), name="Workshop A")
    b = Location(owner=Owner(), name="Workshop B")
    session.add_all([a, b])
    session.flush()
    return session, a, b


def test_an_open_handoff_is_surfaced_by_the_dashboard_query():
    session, location, _other = _seed_two_locations()
    TaskRepository(session).create(
        location.id, "human_handoff",
        related_entity="customer:1", status="open", priority="high",
        details={"reason": "guard-refused reply"},
    )
    session.flush()

    handoffs = WorkshopDashboardQueries(session, location.id).human_handoffs()
    assert len(handoffs) == 1
    assert handoffs[0].details["reason"] == "guard-refused reply"


def test_a_resolved_handoff_no_longer_appears():
    session, location, _other = _seed_two_locations()
    task = TaskRepository(session).create(
        location.id, "human_handoff", status="open", priority="high", details={},
    )
    session.flush()
    assert len(WorkshopDashboardQueries(session, location.id).human_handoffs()) == 1

    TaskRepository(session).resolve(location.id, task.id)
    session.flush()
    assert WorkshopDashboardQueries(session, location.id).human_handoffs() == []
    assert session.get(Task, task.id).status == "resolved"


def test_other_task_types_do_not_leak_into_the_handoff_view():
    session, location, _other = _seed_two_locations()
    TaskRepository(session).create(location.id, "follow_up_reminder", status="open", priority="normal", details={})
    TaskRepository(session).create(location.id, "human_handoff", status="open", priority="high", details={})
    session.flush()

    handoffs = WorkshopDashboardQueries(session, location.id).human_handoffs()
    assert len(handoffs) == 1
    assert handoffs[0].type == "human_handoff"


def test_a_location_cannot_resolve_another_locations_task():
    """Tenant isolation for the new resolve action specifically --
    TaskRepository.resolve() must scope by location_id, not just id."""
    session, location_a, location_b = _seed_two_locations()
    task = TaskRepository(session).create(
        location_a.id, "human_handoff", status="open", priority="high", details={},
    )
    session.flush()

    result = TaskRepository(session).resolve(location_b.id, task.id)
    assert result is None, "resolving with the wrong location_id must not touch another tenant's task"
    assert session.get(Task, task.id).status == "open", "the task must remain untouched"


def test_the_full_loop_escalation_to_dashboard_to_resolution():
    """The complete chain PROVE stage's earlier test only proved half
    of: AIConversationService escalates -> dashboard query surfaces it
    -> staff resolves it -> it's gone. Uses the real escalation tool,
    not a hand-built Task row, so this proves the actual production
    path end to end."""
    from integrations.ai.tools.registry import ServiceAdvisorToolRegistry, ToolContext

    session, location, _other = _seed_two_locations()
    from models.core import Customer
    customer = Customer(location_id=location.id, first_name="A", last_name="B", whatsapp_number="27820001111")
    session.add(customer)
    session.flush()

    ctx = ToolContext(session=session, location_id=location.id, conversation_id=1, customer_id=customer.id)
    registry = ServiceAdvisorToolRegistry(ctx)
    result = registry.execute("escalate_to_human", {"reason": "customer reported an accident", "priority": "high"})
    session.flush()
    assert result["escalated"] is True

    handoffs = WorkshopDashboardQueries(session, location.id).human_handoffs()
    assert len(handoffs) == 1
    assert "accident" in handoffs[0].details["reason"]

    resolved = TaskRepository(session).resolve(location.id, result["task_id"])
    session.flush()
    assert resolved.status == "resolved"
    assert WorkshopDashboardQueries(session, location.id).human_handoffs() == []
