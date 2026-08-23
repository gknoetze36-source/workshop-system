from dataclasses import dataclass


@dataclass(frozen=True)
class SubjectDefinition:
    subject_type: str
    label: str
    required_fields: tuple[str, ...]


SUBJECT_DEFINITIONS = {
    "vehicle": SubjectDefinition(
        subject_type="vehicle",
        label="Vehicle",
        required_fields=("make", "model", "year"),
    ),
    "property": SubjectDefinition(
        subject_type="property",
        label="Property / House",
        required_fields=("address",),
    ),
    "work_site": SubjectDefinition(
        subject_type="work_site",
        label="Work Site",
        required_fields=("address",),
    ),
}


def get_subject_definition(subject_type: str) -> SubjectDefinition:
    try:
        return SUBJECT_DEFINITIONS[subject_type]
    except KeyError as exc:
        raise ValueError(f"unsupported subject type: {subject_type}") from exc
