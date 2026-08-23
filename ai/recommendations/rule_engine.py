from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select

from models.core import Recommendation, Service, ServiceRule, Vehicle


@dataclass(frozen=True)
class ServiceDue:
    service_type: str
    status: str
    due_mileage: int | None
    due_date: datetime | None
    reason: str
    rule_id: int
    source: str = "rule_engine"

    def as_dict(self) -> dict[str, Any]:
        return {
            "service_type": self.service_type,
            "status": self.status,
            "due_mileage": self.due_mileage,
            "due_date": self.due_date.isoformat() if self.due_date else None,
            "reason": self.reason,
            "rule_id": self.rule_id,
            "source": self.source,
        }


class ServiceRuleEngine:
    """Deterministic maintenance scheduler.

    The LLM never decides intervals. Rules are selected by specificity, then
    applied against the vehicle's latest recorded service and current mileage.
    Location-specific rules override global rules with the same service type.
    """

    def __init__(self, session, location_id: int):
        self.session = session
        self.location_id = location_id

    @staticmethod
    def _norm(value: str | None) -> str | None:
        return value.strip().lower() if value else None

    def _rules_for(self, vehicle: Vehicle) -> list[ServiceRule]:
        rows = list(self.session.scalars(
            select(ServiceRule).where(
                ServiceRule.active.is_(True),
                (ServiceRule.location_id == self.location_id) | (ServiceRule.location_id.is_(None)),
            )
        ).all())
        vm, vmodel, veng = map(self._norm, (vehicle.make, vehicle.model, vehicle.engine))
        matching = []
        for rule in rows:
            if rule.make and self._norm(rule.make) != vm:
                continue
            if rule.model and self._norm(rule.model) != vmodel:
                continue
            if rule.engine and self._norm(rule.engine) != veng:
                continue
            specificity = sum(bool(x) for x in (rule.make, rule.model, rule.engine))
            location_specific = rule.location_id == self.location_id
            matching.append((rule.service_type.lower(), specificity, location_specific, rule))

        # One most-specific rule per service type. Location-specific wins ties.
        chosen: dict[str, tuple[int, bool, ServiceRule]] = {}
        for service_type, specificity, location_specific, rule in matching:
            old = chosen.get(service_type)
            if old is None or (specificity, location_specific) > (old[0], old[1]):
                chosen[service_type] = (specificity, location_specific, rule)
        return [v[2] for v in chosen.values()]

    def _latest_services(self, vehicle_id: int) -> dict[str, Service]:
        services = list(self.session.scalars(
            select(Service).where(
                Service.location_id == self.location_id,
                Service.vehicle_id == vehicle_id,
            ).order_by(Service.performed_at.desc())
        ).all())
        latest: dict[str, Service] = {}
        for service in services:
            key = service.service_type.strip().lower()
            latest.setdefault(key, service)
        return latest

    @staticmethod
    def _status_for_mileage(current: int, due: int, interval: int) -> str:
        # Flag at the interval and within 10% of the next interval.
        if current >= due:
            return "due"
        threshold = max(500, int(interval * 0.10))
        return "upcoming" if due - current <= threshold else "not_due"

    def evaluate(self, vehicle_id: int) -> list[ServiceDue]:
        vehicle = self.session.scalar(select(Vehicle).where(
            Vehicle.id == vehicle_id, Vehicle.location_id == self.location_id
        ))
        if not vehicle:
            raise ValueError("vehicle not found")

        latest = self._latest_services(vehicle_id)
        current_mileage = vehicle.mileage
        now = datetime.now(timezone.utc)
        results: list[ServiceDue] = []

        for rule in self._rules_for(vehicle):
            last = latest.get(rule.service_type.strip().lower())
            due_mileage = None
            due_date = None
            statuses: list[str] = []

            if rule.interval_km and current_mileage is not None:
                baseline = last.mileage_at_service if last and last.mileage_at_service is not None else 0
                due_mileage = baseline + rule.interval_km
                statuses.append(self._status_for_mileage(current_mileage, due_mileage, rule.interval_km))

            if rule.interval_months:
                if last:
                    base_date = last.performed_at
                else:
                    # Without service history, do not invent a prior service date.
                    # Use vehicle age only as a conservative first-service anchor.
                    base_date = datetime(vehicle.year, 1, 1, tzinfo=timezone.utc)
                due_date = self._add_months(base_date, rule.interval_months)
                if now >= due_date:
                    statuses.append("due")
                elif due_date - now <= timedelta(days=max(30, int(rule.interval_months * 30 * 0.10))):
                    statuses.append("upcoming")
                else:
                    statuses.append("not_due")

            if not statuses:
                continue
            status = "due" if "due" in statuses else ("upcoming" if "upcoming" in statuses else "not_due")
            if status == "not_due":
                continue

            reasons = []
            if due_mileage is not None and current_mileage is not None:
                reasons.append(f"mileage interval: {current_mileage:,}/{due_mileage:,} km")
            if due_date:
                reasons.append(f"time interval: due {due_date.date().isoformat()}")
            if not last:
                reasons.append("no prior recorded service for this service type")

            results.append(ServiceDue(
                service_type=rule.service_type,
                status=status,
                due_mileage=due_mileage,
                due_date=due_date,
                reason="; ".join(reasons),
                rule_id=rule.id,
            ))
        return sorted(results, key=lambda x: (x.status != "due", x.due_mileage or 10**12, x.due_date or now))

    @staticmethod
    def _add_months(value: datetime, months: int) -> datetime:
        month = value.month - 1 + months
        year = value.year + month // 12
        month = month % 12 + 1
        # Clamp day to the last day of target month without extra dependency.
        if month == 12:
            next_month = datetime(year + 1, 1, 1, tzinfo=value.tzinfo)
        else:
            next_month = datetime(year, month + 1, 1, tzinfo=value.tzinfo)
        last_day = (next_month - timedelta(days=1)).day
        return value.replace(year=year, month=month, day=min(value.day, last_day))

    def persist_due_recommendations(self, vehicle_id: int) -> list[Recommendation]:
        due = self.evaluate(vehicle_id)
        existing = {
            (r.service_type.lower(), r.due_mileage, r.due_date.date() if r.due_date else None): r
            for r in self.session.scalars(select(Recommendation).where(
                Recommendation.location_id == self.location_id,
                Recommendation.vehicle_id == vehicle_id,
                Recommendation.status == "open",
            )).all()
        }
        persisted = []
        for item in due:
            key = (item.service_type.lower(), item.due_mileage, item.due_date.date() if item.due_date else None)
            obj = existing.get(key)
            if obj is None:
                obj = Recommendation(
                    location_id=self.location_id,
                    vehicle_id=vehicle_id,
                    service_type=item.service_type,
                    due_mileage=item.due_mileage,
                    due_date=item.due_date,
                    source=item.source,
                    status="open",
                )
                self.session.add(obj)
            persisted.append(obj)
        self.session.flush()
        return persisted

    # Compatibility with the Phase 4 facade contract.
    def due_services(self, vehicle_id: int) -> dict[str, Any]:
        due = [item.as_dict() for item in self.evaluate(vehicle_id)]
        return {"vehicle_id": vehicle_id, "due_services": due, "source": "deterministic_rule_layer"}
