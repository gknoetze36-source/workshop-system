"""Static pre-GitHub gate for PHANTA.

Run with Python 3.11 before committing. This check is dependency-light and is
intended to catch the repository-level failures that previously reached runtime.
"""
from __future__ import annotations

import ast
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN = re.compile("|".join(["V"+"ANTA", "V"+"anta", "v"+"anta", "automation"+"_engine", "public_location"+"_booking", "awaiting"+"_approval"]))


def main() -> int:
    failures: list[str] = []
    py_files = list(ROOT.rglob("*.py"))
    for path in py_files:
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError as exc:
            failures.append(f"syntax: {path}: {exc}")
            continue
        seen: set[str] = set()
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                if node.name in seen:
                    failures.append(f"duplicate module function: {path}: {node.name}")
                seen.add(node.name)
        text = path.read_text(encoding="utf-8", errors="ignore")
        if FORBIDDEN.search(text):
            failures.append(f"forbidden legacy reference: {path}")

    req = ROOT / "requirements.txt"
    if req.exists() and re.search(r"^(<<<<<<<|=======|>>>>>>>)", req.read_text(encoding="utf-8"), re.M):
        failures.append("requirements.txt contains merge conflict markers")

    for path in (ROOT / "templates").glob("*.html"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "<form" in text and 'name="csrf_token"' not in text:
            failures.append(f"form missing CSRF token: {path}")

    for path in ROOT.rglob("*"):
        if path.is_file() and (path.suffix in {".pyc", ".pyo"} or path.name == ".env"):
            failures.append(f"generated/secret file present: {path}")

    if failures:
        print("PHANTA PRE-GITHUB: FAIL")
        for item in failures:
            print(" -", item)
        return 1
    print(f"PHANTA PRE-GITHUB: PASS ({len(py_files)} Python files checked)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
