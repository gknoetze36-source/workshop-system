from __future__ import annotations

import os

from flask import has_request_context, request, url_for

from .models import Special


def _public_base_url() -> str:
    configured = (
        os.getenv("PHANTA_PUBLIC_BASE_URL", "").strip()
        or os.getenv("PHANTA_BASE_URL", "").strip()
        or os.getenv("PUBLIC_BASE_URL", "").strip()
    )

    if configured:
        return configured.rstrip("/")

    if has_request_context():
        return (request.url_root or "").rstrip("/")

    raise RuntimeError(
        "A public PHANTA base URL is required when generating "
        "Flyer Lady tracking links outside a request context"
    )


def tracking_url(special_id: int) -> str:
    if has_request_context():
        path = url_for(
            "flyer_lady.redirect_special",
            special_id=special_id,
        )
    else:
        path = f"/dashboard/flyer-lady/l/{special_id}"

    return f"{_public_base_url()}{path}"


def build_caption(special: Special) -> str:
    return (
        f"{special.text.strip()}\n\n"
        f"Book here: {tracking_url(special.id)}"
    )