from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, date as date_type, time as time_type
from decimal import Decimal
from typing import Any, Callable
import re

from sqlalchemy import select, and_

from models.core import (
    Customer, Vehicle, Booking, Service, Recommendation,
    FollowUp, Task, AuditLog, ToolExecution,
)
from integrations.ai.providers.base_provider import ToolDefinition
from ai.booking.availability import BookingAvailabilityService, OperatingWindow, WorkshopSchedule
from ai.booking.service import BookingService
from ai.booking.confirmation import BookingConfirmationService


class ToolExecutionError(ValueError):
    pass


@dataclass
class ToolContext:
    session: Any
    location_id: int
    conversation_id: int
    customer_id: int | None = None


def _clean_text(value: Any, max_len: int = 500) -> str:
    value = str(value or "").strip()
    if not value:
        raise ToolExecutionError("required text is missing")
    return value[:max_len]


def _positive_int(value: Any, field: str) -> int:
    try:
        n = int(value)
    except Exception as exc:
        raise ToolExecutionError(f"{field} must be an integer") from exc
    if n <= 0:
        raise ToolExecutionError(f"{field} must be positive")
    return n


class ServiceAdvisorToolRegistry:
    """Server-side allowlist of Service Advisor tools.

    The model can request a tool, but never supplies SQL or arbitrary entity IDs
    without location/ownership validation. Every execution is recorded.
    """

    def __init__(self, context: ToolContext, *, send_message: Callable | None = None,
                 booking_service: BookingService | None = None, current_user_text: str | None = None):
        self.ctx = context
        self.send_message = send_message
        self.booking_service = booking_service
        self.current_user_text = current_user_text
        self._handlers = {
            "capture_customer_context": self.capture_customer_context,
            "get_customer": self.get_customer,
            "get_vehicle": self.get_vehicle,
            "get_vehicle_history": self.get_vehicle_history,
            "get_available_booking_dates": self.get_available_booking_dates,
            "create_booking": self.create_booking,
            "confirm_booking": self.confirm_booking,
            "get_due_services": self.get_due_services,
            "create_follow_up": self.create_follow_up,
            "send_message": self._send_message,
            "escalate_to_human": self.escalate_to_human,
        }

    def definitions(self) -> list[ToolDefinition]:
        return [
            ToolDefinition("capture_customer_context", "Capture only newly learned customer, vehicle, symptom and urgency facts.", {
                "type":"object","properties":{
                    "first_name":{"type":"string"},"last_name":{"type":"string"},
                    "vehicle_id":{"type":"integer"},"make":{"type":"string"},"model":{"type":"string"},
                    "year":{"type":"integer"},"mileage":{"type":"integer"},
                    "problem":{"type":"string"},"urgency":{"type":"string","enum":["routine","soon","urgent","unsafe_to_drive"]}
                },"additionalProperties":False}),
            ToolDefinition("get_customer", "Get the current location's customer record and known vehicles.", {
                "type":"object","properties":{"customer_id":{"type":"integer"}},"additionalProperties":False}),
            ToolDefinition("get_vehicle", "Get a vehicle belonging to the current customer/location.", {
                "type":"object","properties":{"vehicle_id":{"type":"integer"}},"required":["vehicle_id"],"additionalProperties":False}),
            ToolDefinition("get_vehicle_history", "Get recent service, booking and recommendation history for a vehicle.", {
                "type":"object","properties":{"vehicle_id":{"type":"integer"},"limit":{"type":"integer"}},"required":["vehicle_id"],"additionalProperties":False}),
            ToolDefinition("get_available_booking_dates", "Check whether a morning arrival is available on a requested date. Never expose or ask for an exact time; customers bring the vehicle when the workshop opens.", {
                "type":"object","properties":{"date":{"type":"string"}},"required":["date"],"additionalProperties":False}),
            ToolDefinition("create_booking", "Create a PENDING booking request for a date and morning arrival. This never confirms the booking; an explicit customer yes/no must be recorded by confirm_booking.", {
                "type":"object","properties":{
                    "vehicle_id":{"type":"integer"},"booking_date":{"type":"string"},
                    "service_type":{"type":"string"},"notes":{"type":"string"}
                },"required":["vehicle_id","booking_date","service_type"],"additionalProperties":False}),
            ToolDefinition("confirm_booking", "Confirm or decline a pending booking only from the customer's current explicit yes/no message. This is booking confirmation only, never repair or price authorization.", {
                "type":"object","properties":{
                    "booking_id":{"type":"integer"},"raw_message":{"type":"string"}
                },"required":["booking_id","raw_message"],"additionalProperties":False}),
            ToolDefinition("get_due_services", "Return maintenance recommendations from the deterministic rule layer.", {
                "type":"object","properties":{"vehicle_id":{"type":"integer"}},"required":["vehicle_id"],"additionalProperties":False}),
            ToolDefinition("create_follow_up", "Schedule a deterministic follow-up task.", {
                "type":"object","properties":{"type":{"type":"string"},"scheduled_for":{"type":"string"},"channel":{"type":"string"},"payload":{"type":"object"}},"required":["type","scheduled_for"],"additionalProperties":False}),
            ToolDefinition("send_message", "Send a message through PHANTA's configured messaging layer after server-side policy checks.", {
                "type":"object","properties":{"text":{"type":"string"},"channel":{"type":"string","enum":["whatsapp","sms","email"]}},"required":["text"],"additionalProperties":False}),
            ToolDefinition("escalate_to_human", "Create a human handoff task when the AI is out of depth, customer is upset, or a financial/legal decision needs staff confirmation.", {
                "type":"object","properties":{"reason":{"type":"string"},"priority":{"type":"string","enum":["normal","high","urgent"]}},"required":["reason"],"additionalProperties":False}),
        ]

    def execute(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if name not in self._handlers:
            raise ToolExecutionError(f"tool {name!r} is not allowed")
        started = datetime.now(timezone.utc)
        try:
            result = self._handlers[name](**(arguments or {}))
            success = True
            return result
        except Exception as exc:
            success = False
            result = {"error": str(exc)}
            raise
        finally:
            elapsed = int((datetime.now(timezone.utc) - started).total_seconds() * 1000)
            self.ctx.session.add(ToolExecution(
                location_id=self.ctx.location_id, conversation_id=self.ctx.conversation_id,
                tool_name=name, arguments=arguments or {}, result=result if success else {"error": str(result.get("error",""))},
                success=success, latency_ms=elapsed,
            ))
            self.ctx.session.flush()

    def _customer(self, customer_id: int | None = None) -> Customer:
        cid = customer_id or self.ctx.customer_id
        if not cid:
            raise ToolExecutionError("customer_id is required")
        row = self.ctx.session.scalar(select(Customer).where(Customer.id == _positive_int(cid,"customer_id"), Customer.location_id == self.ctx.location_id, Customer.deleted_at.is_(None)))
        if not row:
            raise ToolExecutionError("customer not found")
        return row

    def _vehicle(self, vehicle_id: int) -> Vehicle:
        row = self.ctx.session.scalar(select(Vehicle).where(Vehicle.id == _positive_int(vehicle_id,"vehicle_id"), Vehicle.location_id == self.ctx.location_id))
        if not row:
            raise ToolExecutionError("vehicle not found")
        if self.ctx.customer_id and row.customer_id != self.ctx.customer_id:
            raise ToolExecutionError("vehicle does not belong to current customer")
        return row

    def capture_customer_context(self, **data):
        customer = self._customer()
        if "first_name" in data: customer.first_name = _clean_text(data["first_name"],100)
        if "last_name" in data: customer.last_name = _clean_text(data["last_name"],100)
        vehicle = None
        if data.get("vehicle_id"):
            vehicle = self._vehicle(data["vehicle_id"])
        elif data.get("make") and data.get("model") and data.get("year"):
            year = _positive_int(data["year"], "year")
            if year < 1950 or year > datetime.now().year + 1: raise ToolExecutionError("vehicle year is outside allowed range")
            vehicle = Vehicle(location_id=self.ctx.location_id, customer_id=customer.id, make=_clean_text(data["make"],100),
                              model=_clean_text(data["model"],100), year=year, mileage=int(data["mileage"]) if data.get("mileage") is not None else None)
            self.ctx.session.add(vehicle); self.ctx.session.flush()
        # Store conversationally useful facts without pretending problem/urgency are vehicle facts.
        if data.get("problem") or data.get("urgency"):
            customer.notes = ((customer.notes or "") + "\n" + f"Current issue: {str(data.get('problem','')).strip()} | urgency: {str(data.get('urgency','')).strip()}").strip()[-4000:]
        self.ctx.session.add(AuditLog(location_id=self.ctx.location_id, actor="ai", action="customer_context_captured", entity_type="customer", entity_id=str(customer.id), after={"vehicle_id": getattr(vehicle,"id",None),"urgency":data.get("urgency")}))
        return {"customer_id": customer.id, "vehicle_id": getattr(vehicle,"id",None), "captured": True}

    def get_customer(self, customer_id=None):
        c=self._customer(customer_id)
        return {"id":c.id,"first_name":c.first_name,"last_name":c.last_name,"whatsapp_number":c.whatsapp_number,
                "email":c.email,"vehicles":[{"id":v.id,"make":v.make,"model":v.model,"year":v.year,"mileage":v.mileage} for v in c.vehicles if v.location_id==self.ctx.location_id]}

    def get_vehicle(self, vehicle_id):
        v=self._vehicle(vehicle_id)
        return {"id":v.id,"customer_id":v.customer_id,"make":v.make,"model":v.model,"year":v.year,"mileage":v.mileage,"engine":v.engine,"transmission":v.transmission,"registration":v.registration}

    def get_vehicle_history(self, vehicle_id, limit=20):
        v=self._vehicle(vehicle_id); limit=max(1,min(int(limit or 20),50))
        services=self.ctx.session.scalars(select(Service).where(Service.location_id==self.ctx.location_id,Service.vehicle_id==v.id).order_by(Service.performed_at.desc()).limit(limit)).all()
        bookings=self.ctx.session.scalars(select(Booking).where(Booking.location_id==self.ctx.location_id,Booking.vehicle_id==v.id).order_by(Booking.start_time.desc()).limit(limit)).all()
        recs=self.ctx.session.scalars(select(Recommendation).where(Recommendation.location_id==self.ctx.location_id,Recommendation.vehicle_id==v.id).order_by(Recommendation.id.desc()).limit(limit)).all()
        return {"vehicle":self.get_vehicle(v.id),"services":[{"id":s.id,"type":s.service_type,"performed_at":s.performed_at.isoformat(),"mileage":s.mileage_at_service,"notes":s.notes} for s in services],
                "bookings":[{"id":b.id,"start":b.start_time.isoformat(),"end":b.end_time.isoformat(),"status":b.status,"service_type":b.service_type} for b in bookings],
                "recommendations":[{"id":r.id,"service_type":r.service_type,"due_date":r.due_date.isoformat() if r.due_date else None,"due_mileage":r.due_mileage,"status":r.status} for r in recs]}

    def _booking_service(self) -> BookingService:
        if self.booking_service is not None:
            return self.booking_service
        # Local/test fallback. Production should inject workshop-specific hours.
        hours = {d: [OperatingWindow(time_type(8, 0), time_type(17, 0))] for d in range(5)}
        schedule = WorkshopSchedule(hours)
        return BookingService(self.ctx.session, BookingAvailabilityService(self.ctx.session, schedule))

    def get_available_booking_dates(self, date):
        try:
            day = date_type.fromisoformat(str(date))
        except ValueError as exc:
            raise ToolExecutionError("date must be YYYY-MM-DD") from exc
        service = self._booking_service()
        # Phase 15 deliberately uses the workshop opening as the customer-facing
        # arrival point. Exact times remain internal scheduling data.
        opening = datetime.combine(day, time_type(8, 0), tzinfo=timezone.utc)
        duration = timedelta(minutes=60)
        try:
            service.availability.assert_available(self.ctx.location_id, opening, opening + duration)
            available = True
        except Exception:
            available = False
        return {"date": str(day), "arrival": "morning", "workshop_opening": "when the workshop opens", "available": available}

    def create_booking(self, vehicle_id, booking_date, service_type, notes=None):
        v = self._vehicle(vehicle_id)
        customer = self._customer(v.customer_id)
        try:
            day = date_type.fromisoformat(str(booking_date))
        except ValueError as exc:
            raise ToolExecutionError("booking_date must be YYYY-MM-DD") from exc
        opening = datetime.combine(day, time_type(8, 0), tzinfo=timezone.utc)
        end = opening + timedelta(minutes=60)
        service = self._booking_service()
        try:
            booking = service.create_booking(
                location_id=self.ctx.location_id,
                customer_id=customer.id,
                vehicle_id=v.id,
                start_time=opening,
                end_time=end,
                service_type=_clean_text(service_type, 100),
                source="whatsapp",
                notes=(notes or "")[:2000],
                conversation_id=None,
            )
        except Exception as exc:
            raise ToolExecutionError(str(exc)) from exc
        # BookingService creates pending bookings. Do not confirm here.
        return {
            "booking_id": booking.id,
            "status": booking.status,
            "date": str(day),
            "arrival": "morning",
            "customer_confirmation_required": True,
        }

    def confirm_booking(self, booking_id, raw_message):
        text = str(raw_message or "").strip()
        if self.current_user_text is not None and text != self.current_user_text.strip():
            raise ToolExecutionError("confirmation evidence must be the customer's current message")
        booking = self.ctx.session.scalar(select(Booking).where(
            Booking.id == _positive_int(booking_id, "booking_id"),
            Booking.location_id == self.ctx.location_id,
            Booking.customer_id == self.ctx.customer_id,
        ))
        if not booking:
            raise ToolExecutionError("booking not found")
        try:
            record = BookingConfirmationService(self.ctx.session).confirm(
                location_id=self.ctx.location_id,
                customer_id=self.ctx.customer_id,
                booking_id=booking.id,
                raw_message=text,
                channel="whatsapp",
            )
            # Lifecycle Phase 16: confirmation is a booking event, not repair
            # authorisation. If the conversation runtime supplied a delivery
            # callback, send the fixed booking-confirmed message immediately.
            if record.decision == "confirmed" and self.send_message:
                self.send_message(
                    self.ctx.customer_id,
                    f"Your vehicle is booked for {booking.start_time.date().isoformat()} morning. Please bring the vehicle when the workshop opens.",
                    "whatsapp",
                )
        except ValueError as exc:
            raise ToolExecutionError(str(exc)) from exc
        return {
            "booking_confirmation_id": record.id,
            "booking_id": booking.id,
            "decision": record.decision,
            "booking_status": booking.status,
            "date": booking.start_time.date().isoformat(),
            "arrival": "morning",
            "booking_confirmation_recorded": True,
        }

    def get_due_services(self, vehicle_id):
        """Return deterministic due/upcoming maintenance and persist recommendations.

        The rule engine is the only source of maintenance intervals. Calling this
        tool may create/update idempotent open recommendation records, but it
        never asks the LLM to calculate an interval.
        """
        v = self._vehicle(vehicle_id)
        from ai.recommendations.rule_engine import ServiceRuleEngine
        engine = ServiceRuleEngine(self.ctx.session, self.ctx.location_id)
        result = engine.due_services(v.id)
        persisted = engine.persist_due_recommendations(v.id)
        result["recommendation_ids"] = [row.id for row in persisted]
        return result

    def create_follow_up(self, type, scheduled_for, channel="whatsapp", payload=None):
        c=self._customer()
        try: when=datetime.fromisoformat(str(scheduled_for).replace("Z","+00:00"))
        except ValueError as exc: raise ToolExecutionError("scheduled_for must be ISO-8601") from exc
        if when <= datetime.now(timezone.utc): raise ToolExecutionError("follow-up must be in the future")
        f=FollowUp(location_id=self.ctx.location_id,customer_id=c.id,type=_clean_text(type,50),scheduled_for=when,status="scheduled",channel=channel,payload=payload or {})
        self.ctx.session.add(f); self.ctx.session.flush()
        return {"follow_up_id":f.id,"status":"scheduled"}

    def _send_message(self, text, channel="whatsapp"):
        text=_clean_text(text,4000)
        if not self.send_message:
            raise ToolExecutionError("messaging service is not configured")
        if channel not in {"whatsapp","sms","email"}: raise ToolExecutionError("unsupported channel")
        result=self.send_message(self.ctx.customer_id, text, channel)
        return {"sent":True,"channel":channel,"provider_result":result or {}}

    def escalate_to_human(self, reason, priority="normal"):
        c=self._customer()
        t=Task(location_id=self.ctx.location_id,type="human_handoff",related_entity=f"customer:{c.id}",status="open",priority=priority,details={"reason":_clean_text(reason,1000),"conversation_id":self.ctx.conversation_id})
        self.ctx.session.add(t); self.ctx.session.flush()
        return {"task_id":t.id,"escalated":True,"priority":priority}
