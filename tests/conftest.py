import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import os
os.environ.setdefault("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
# WhatsAppMetaConfig.from_env() validates these unconditionally (numeric
# APP_ID, >=16-char APP_SECRET that isn't a known placeholder). Several real
# call paths -- routes/bookings.py's change_booking_status() among them --
# construct a real GraphApiClient(WhatsAppMetaConfig.from_env()) even on the
# failure branch (see ai/communications/lifecycle.py's booking_missed()),
# so any test exercising that route needs these set regardless of whether
# it cares about WhatsApp sending at all. Set globally here rather than
# per-file so a test written before a later change adds a new call path
# into this validation doesn't need updating just to keep importing.
os.environ.setdefault("META_WHATSAPP_APP_ID", "123456789012345")
os.environ.setdefault("META_WHATSAPP_APP_SECRET", "test-only-not-a-real-app-secret-value")

# The suite drives hundreds of requests from a single client address, which
# would otherwise trip the now-enforced rate limits. Limiter behaviour is
# covered by its own dedicated test rather than by every route test.
os.environ.setdefault("RATELIMIT_ENABLED", "false")
