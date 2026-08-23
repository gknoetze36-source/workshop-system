from dataclasses import dataclass


@dataclass(frozen=True)
class IndustryDefinition:
    key: str
    customer_mode: str
    subject_type: str | None
    subject_label: str | None


INDUSTRIES = {
    "salon": IndustryDefinition("salon", "customer_only", None, None),
    "dentist": IndustryDefinition("dentist", "customer_only", None, None),
    "restaurant": IndustryDefinition("restaurant", "customer_only", None, None),
    "barber": IndustryDefinition("barber", "customer_only", None, None),
    "workshop": IndustryDefinition("workshop", "customer_subject", "vehicle", "Vehicle"),
    "plumber": IndustryDefinition("plumber", "customer_subject", "property", "Property / House"),
    "construction": IndustryDefinition("construction", "customer_subject", "work_site", "Work Site"),
}


def get_industry(key: str) -> IndustryDefinition:
    try:
        return INDUSTRIES[key]
    except KeyError as exc:
        raise ValueError(f"unsupported industry: {key}") from exc
