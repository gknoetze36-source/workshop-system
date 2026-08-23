"""Small resolver for location-selected industry configuration."""
from .workshop import PROFILE as WORKSHOP
from .salon import PROFILE as SALON
from .barber import PROFILE as BARBER

PROFILES = {p["key"]: p for p in (WORKSHOP, SALON, BARBER)}


def get_industry_profile(industry: str) -> dict:
    key = (industry or "").strip().lower()
    try:
        return PROFILES[key]
    except KeyError as exc:
        raise ValueError(f"industry '{key}' is not configured") from exc
