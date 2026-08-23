"""Run the Phase 12 local validation suite."""
from __future__ import annotations
import subprocess
import sys

if __name__ == "__main__":
    raise SystemExit(subprocess.call([sys.executable, "-m", "pytest", "-q", "tests/integration/test_phase12_service_advisor.py", "tests/unit/test_meta_webhook_phase8.py"]))
