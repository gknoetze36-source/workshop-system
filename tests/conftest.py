import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import os
os.environ.setdefault("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")

# The suite drives hundreds of requests from a single client address, which
# would otherwise trip the now-enforced rate limits. Limiter behaviour is
# covered by its own dedicated test rather than by every route test.
os.environ.setdefault("RATELIMIT_ENABLED", "false")
