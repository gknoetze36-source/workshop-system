from app.core.domain.industry.catalog import IndustryDefinition
from app.core.domain.models import Booking


def create_booking(
    *,
    booking_id: int,
    business_id: int,
    customer_id: int,
    industry: IndustryDefinition,
    subject_id: int | None = None,
) -> Booking:
    if industry.customer_mode == "customer_only":
        if subject_id is not None:
            raise ValueError("customer-only industries cannot attach a subject")
        return Booking(
            id=booking_id,
            business_id=business_id,
            customer_id=customer_id,
            subject_id=None,
            subject_type=None,
        )

    if industry.customer_mode == "customer_subject":
        if subject_id is None:
            raise ValueError(
                f"{industry.key} requires a {industry.subject_type} subject"
            )
        return Booking(
            id=booking_id,
            business_id=business_id,
            customer_id=customer_id,
            subject_id=subject_id,
            subject_type=industry.subject_type,
        )

    raise ValueError(f"unsupported customer mode: {industry.customer_mode}")
