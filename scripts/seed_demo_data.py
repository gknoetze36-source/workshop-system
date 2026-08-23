"""Seed a demo workshop so the reception dashboard (/dashboard) has real data
to show, instead of loading empty.

Usage:
    python -m scripts.seed_demo_data

Safe to run once. Refuses to run twice against the same database (checks
for an existing location with the demo slug first) so you don't end up with
duplicate demo workshops piling up every redeploy.

Creates:
    - 1 Owner + 1 Location ("PHANTA Demo Workshop", industry=workshop)
    - 1 reception-role login: demo.reception@phanta.example / DemoPass123!
    - 3 customers, each with 1 vehicle
    - Bookings covering every section of the dashboard:
        * one pending today with no confirmation yet -> today's bookings
          AND the booking-requests-needing-confirmation queue
        * one checked_in -> vehicles waiting
        * one in_progress with an end_time in the past -> overdue vehicles
    - 1 WhatsApp conversation with an unanswered inbound message

Does not touch Meta/Paystack connections or subscriptions -- the dashboard
already renders "not_connected" / "not_configured" for those correctly when
absent, which is itself useful to see in a demo.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from werkzeug.security import generate_password_hash

from database import execute_db, query_db, utc_now, get_session
from models.core import Owner, Location, Customer, Vehicle, Booking, Conversation, Message

DEMO_SLUG = "phanta-demo-workshop"
DEMO_EMAIL = "demo.reception@phanta.example"
DEMO_PASSWORD = "DemoPass123!"


def _already_seeded() -> bool:
    return bool(query_db(
        "SELECT l.id FROM locations l WHERE l.slug = %s LIMIT 1",
        (DEMO_SLUG,), one=True,
    ))


def seed() -> dict:
    if _already_seeded():
        existing = query_db("SELECT id FROM locations WHERE slug = %s", (DEMO_SLUG,), one=True)
        print(f"Demo workshop already exists (location_id={existing['id']}). Nothing to do.")
        return {"already_seeded": True, "location_id": existing["id"]}

    session = get_session()
    try:
        owner = Owner(name="Demo Owner", email=DEMO_EMAIL, active=True)
        location = Location(
            owner=owner,
            name="PHANTA Demo Workshop",
            slug=DEMO_SLUG,
            industry="workshop",
            active=True,
        )
        session.add(location)
        session.flush()  # get location.id / owner.id without committing yet

        customers = [
            Customer(location_id=location.id, first_name="Thabo", last_name="Nkosi",
                      whatsapp_number="+27821110001"),
            Customer(location_id=location.id, first_name="Sarah", last_name="van der Merwe",
                      whatsapp_number="+27821110002"),
            Customer(location_id=location.id, first_name="Ahmed", last_name="Patel",
                      whatsapp_number="+27821110003"),
        ]
        session.add_all(customers)
        session.flush()

        vehicles = [
            Vehicle(location_id=location.id, customer_id=customers[0].id,
                     make="Toyota", model="Corolla", year=2019, registration="CA 123-456"),
            Vehicle(location_id=location.id, customer_id=customers[1].id,
                     make="VW", model="Polo", year=2021, registration="CA 789-012"),
            Vehicle(location_id=location.id, customer_id=customers[2].id,
                     make="Ford", model="Ranger", year=2018, registration="CA 345-678"),
        ]
        session.add_all(vehicles)
        session.flush()

        now = datetime.now(timezone.utc)
        upcoming_today = now + timedelta(hours=2)

        bookings = [
            # Pending, later today, no confirmation yet -> shows in both
            # today's bookings and the booking-requests queue. Scheduled
            # relative to "now" (not a fixed 9am) so it never spuriously
            # looks overdue depending on what time the seed script runs.
            Booking(
                location_id=location.id, customer_id=customers[0].id, vehicle_id=vehicles[0].id,
                start_time=upcoming_today, end_time=upcoming_today + timedelta(hours=1),
                status="pending", service_type="Oil change + filter", source="whatsapp",
            ),
            # Checked in -> vehicles waiting.
            Booking(
                location_id=location.id, customer_id=customers[1].id, vehicle_id=vehicles[1].id,
                start_time=now - timedelta(hours=1), end_time=now + timedelta(hours=1),
                status="checked_in", service_type="Brake pads", source="whatsapp",
            ),
            # In progress but past its scheduled end -> overdue.
            Booking(
                location_id=location.id, customer_id=customers[2].id, vehicle_id=vehicles[2].id,
                start_time=now - timedelta(hours=4), end_time=now - timedelta(hours=1),
                status="in_progress", service_type="Full service", source="phone",
            ),
        ]
        session.add_all(bookings)
        session.flush()

        conversation = Conversation(
            location_id=location.id, customer_id=customers[0].id, channel="whatsapp",
        )
        session.add(conversation)
        session.flush()
        session.add(Message(
            location_id=location.id, conversation_id=conversation.id,
            direction="inbound", channel="whatsapp",
            body="Hi, is my Corolla still on for 9am today?",
            status="received",
        ))

        session.commit()
        location_id = location.id
        owner_id = owner.id
    finally:
        session.close()

    # The users table is on the raw execute_db/query_db path (auth_service
    # authenticates against it directly), not the ORM -- mirror the shape
    # routes/auth.py uses so this login works exactly like a real one.
    execute_db(
        """INSERT INTO users
           (username,email,password,password_hash,full_name,role,owner_id,location_id,
            active,must_reset_password,created_at,updated_at)
           VALUES (%s,%s,%s,%s,%s,'reception',%s,%s,TRUE,FALSE,%s,%s)""",
        (DEMO_EMAIL, DEMO_EMAIL, "", generate_password_hash(DEMO_PASSWORD),
         "Demo Reception", owner_id, location_id, utc_now(), utc_now()),
    )

    print("Demo workshop seeded.")
    print(f"  location_id: {location_id}")
    print(f"  login:       {DEMO_EMAIL} / {DEMO_PASSWORD}")
    print("  Go to /login, sign in, and you'll land on the reception dashboard.")
    return {"already_seeded": False, "location_id": location_id, "login_email": DEMO_EMAIL}


if __name__ == "__main__":
    seed()
