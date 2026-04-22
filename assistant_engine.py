from ai_engine import classify_message
from platform_helpers import insert_booking, fetch_all
from database import fetch_one, execute_db, utc_now
import json

def get_session(phone):
    return fetch_one("SELECT * FROM chat_sessions WHERE phone=%s", (phone,))

def save_session(phone, branch_id, state, context):
    existing = get_session(phone)

    if existing:
        execute_db(
            "UPDATE chat_sessions SET state=%s, context=%s, updated_at=%s WHERE phone=%s",
            (state, json.dumps(context), utc_now(), phone)
        )
    else:
        execute_db(
            "INSERT INTO chat_sessions (phone, branch_id, state, context, updated_at) VALUES (%s,%s,%s,%s,%s)",
            (phone, branch_id, state, json.dumps(context), utc_now())
        )

def assistant_reply(phone, message, branch):
    session = get_session(phone)

    # EXISTING FLOW
    if session:
        state = session["state"]
        context = json.loads(session["context"] or "{}")

        if state == "awaiting_date":
            context["date"] = message
            save_session(phone, branch["id"], "awaiting_confirmation", context)
            return f"Confirm booking on {message}? (yes/no)", True

        if state == "awaiting_confirmation":
            if "yes" in message.lower():
                ref = insert_booking(
                    branch,
                    {
                        "first_name": "Client",
                        "phone": phone,
                        "service": context["service"],
                        "scheduled_date": context["date"]
                    },
                    "WhatsApp",
                    "Confirmed"
                )
                return f"Booking confirmed. Ref: {ref}", True

    # NEW MESSAGE
    intent = classify_message(message)

    if intent == "pricing":
        return "Pricing logic here", True

    if intent in ["booking", "repair"]:
        save_session(phone, branch["id"], "awaiting_date", {"service": "Service"})
        return "What date would you like?", True

    return None, False
