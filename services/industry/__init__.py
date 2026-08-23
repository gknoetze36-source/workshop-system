"""Industry-specific configuration and workflows.

Universal services depend on this package only for the selected location industry.
Industry modules own their own defaults; they do not own authentication,
payments, messaging transport, or location isolation.
"""
from .registry import get_industry_profile

__all__ = ["get_industry_profile"]
