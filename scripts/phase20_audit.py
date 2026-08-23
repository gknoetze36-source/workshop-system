"""Run the repeatable local Phase 20 engineering checks."""
from __future__ import annotations

import ast
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
IGNORED_DIRS = {".git", "__pycache__", ".pytest_cache", ".venv", "venv"}


def python_files():
    for path in ROOT.rglob("*.py"):
        if not any(part in IGNORED_DIRS for part in path.parts):
            yield path


def check_syntax():
    errors = []
    for path in python_files():
        try:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except Exception as exc:  # pragma: no cover - exercised by broken source
            errors.append(f"{path}: {exc}")
    return errors


def check_secrets():
    patterns = [
        re.compile(r"sk-[A-Za-z0-9]{20,}"),
        re.compile(r"AKIA[0-9A-Z]{16}"),
        re.compile(r"(?:PAYSTACK_SECRET_KEY|META_APP_SECRET|OPENAI_API_KEY|ANTHROPIC_API_KEY|GOOGLE_CLIENT_SECRET)[ \t]*=[ \t]*[^\s$<{][^\n]*"),
    ]
    hits = []
    for path in ROOT.rglob("*"):
        if not path.is_file() or any(part in IGNORED_DIRS for part in path.parts):
            continue
        if path.suffix.lower() in {".pyc", ".db", ".sqlite", ".sqlite3"}:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for pattern in patterns:
            if pattern.search(text):
                hits.append(str(path))
                break
    return sorted(set(hits))


def run(*args):
    return subprocess.run(args, cwd=ROOT, text=True, capture_output=True)


def main():
    failures = []
    syntax = check_syntax()
    print(f"syntax: {'PASS' if not syntax else 'FAIL'}")
    for item in syntax:
        print(f"  {item}")
    if syntax:
        failures.append("syntax")

    secrets = check_secrets()
    print(f"secret-pattern scan: {'PASS' if not secrets else 'REVIEW'}")
    for item in secrets:
        print(f"  review: {item}")

    compile_result = run(sys.executable, "-m", "compileall", "-q", ".")
    print(f"compileall: {'PASS' if compile_result.returncode == 0 else 'FAIL'}")
    if compile_result.returncode:
        print(compile_result.stdout, compile_result.stderr)
        failures.append("compileall")

    pytest_result = run(sys.executable, "-m", "pytest", "-q")
    print(f"pytest: {'PASS' if pytest_result.returncode == 0 else 'FAIL'}")
    print(pytest_result.stdout.strip())
    if pytest_result.returncode:
        print(pytest_result.stderr.strip())
        failures.append("pytest")

    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
