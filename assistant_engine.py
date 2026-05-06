import json

from ai_engine import classify_message
from platform_helpers import branch_by_id, find_service_price, insert_booking
from database import query_db, execute_db, utc_now

def get_session(phone):
    return query_db("SELECT * FROM chat_sessions WHERE phone=%s", (phone,), one=True)

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
    cleaned_message = (message or "").strip()
    customer_name = "Client"
    metadata = {"intent": "unknown", "service_type": "", "conversation_state": "NEW_INQUIRY"}

    # EXISTING FLOW
    if session:
        state = session["state"]
        context = json.loads(session["context"] or "{}")
        metadata["service_type"] = context.get("service") or ""

        if state == "awaiting_date":
            context["date"] = cleaned_message
            save_session(phone, branch["id"], "awaiting_confirmation", context)
            metadata["intent"] = "booking"
            metadata["conversation_state"] = "BOOKING_PENDING"
            return f"Confirm booking on {cleaned_message}? (yes/no)", True, metadata

        if state == "awaiting_confirmation":
            if "yes" in cleaned_message.lower():
                ref = insert_booking(
                    branch,
                    {
                        "first_name": customer_name,
                        "phone": phone,
                        "service": context["service"],
                        "scheduled_date": context["date"]
                    },
                    "WhatsApp",
                    "Confirmed"
                )
                metadata["intent"] = "booking"
                metadata["conversation_state"] = "BOOKED"
                return f"Booking confirmed. Ref: {ref}", True, metadata
            if "no" in cleaned_message.lower():
                save_session(phone, branch["id"], "closed", context)
                metadata["intent"] = "decline"
                metadata["conversation_state"] = "LOST"
                return "No problem. Send us another preferred date when you're ready.", True, metadata

    # NEW MESSAGE
    intent = classify_message(cleaned_message)
    metadata["intent"] = intent

    if intent == "pricing":
        price = find_service_price(branch["franchise_id"], branch["id"], cleaned_message.title())
        metadata["service_type"] = cleaned_message.title()
        metadata["conversation_state"] = "ENGAGED"
        if price:
            return f"Our fixed price for {price['service_name']} is R{float(price['price_amount'] or 0):.2f}.", True, metadata
        return "We have saved your message for the branch team to price manually. They will reply from the dashboard.", False, metadata

    if intent in ["booking", "repair"]:
        service_name = "Vehicle Inspection For Repairs" if intent == "repair" else "General Service"
        save_session(phone, branch["id"], "awaiting_date", {"service": service_name})
        metadata["service_type"] = service_name
        metadata["conversation_state"] = "BOOKING_PENDING"
        return "What date would you like?", True, metadata

    metadata["conversation_state"] = "ENGAGED" if "?" in cleaned_message or cleaned_message else "NEW_INQUIRY"
    return None, False, metadata
