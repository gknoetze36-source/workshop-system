"""Phone normalization shared by booking, messaging and customer flows."""
import re


def normalize_phone(phone):
    digits = re.sub(r"\D", "", str(phone or ""))
    if digits.startswith("0") and len(digits) == 10:
        return f"27{digits[1:]}"
    if digits.startswith("27"):
        return digits
    if len(digits) == 9:
        return f"27{digits}"
    return digits
