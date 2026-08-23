from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Customer:
    id: int
    business_id: int
    name: str
    phone: str | None = None
    email: str | None = None


@dataclass(frozen=True)
class Subject:
    id: int
    business_id: int
    subject_type: str
    display_name: str
    metadata: dict[str, Any]


@dataclass(frozen=True)
class Booking:
    id: int
    business_id: int
    customer_id: int
    subject_id: int | None
    subject_type: str | None
