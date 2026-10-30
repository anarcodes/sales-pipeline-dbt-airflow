#!/usr/bin/env python3
"""Create a local .env from .env.example with freshly generated secrets.

Usage:
    python scripts/init_env.py           # create .env (refuses to overwrite)
    python scripts/init_env.py --force   # regenerate everything (new passwords!)

Only the standard library is used, so it runs on any machine with Python 3.8+.
"""
from __future__ import annotations

import argparse
import base64
import os
import secrets
import sys
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parent.parent
EXAMPLE = ROOT / ".env.example"
TARGET = ROOT / ".env"
PLACEHOLDER = "CHANGE_ME"


def _fernet_key() -> str:
    """A Fernet key is 32 random bytes, url-safe base64 encoded."""
    return base64.urlsafe_b64encode(os.urandom(32)).decode()


# Passwords are hex on purpose: they are embedded in connection URIs,
# where characters like '@', '/', ':' or '#' would break parsing.
GENERATORS: dict[str, Callable[[], str]] = {
    "AIRFLOW_FERNET_KEY": _fernet_key,
    "AIRFLOW_JWT_SECRET": lambda: secrets.token_urlsafe(48),
    "AIRFLOW_API_SECRET_KEY": lambda: secrets.token_urlsafe(48),
    "AIRFLOW_ADMIN_PASSWORD": lambda: secrets.token_hex(8),
    "AIRFLOW_DB_PASSWORD": lambda: secrets.token_hex(16),
    "DWH_PASSWORD": lambda: secrets.token_hex(16),
}


def build_env(template: str) -> tuple[str, dict[str, str]]:
    """Return (rendered .env text, generated key->value map)."""
    out_lines: list[str] = []
    generated: dict[str, str] = {}

    for line in template.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            out_lines.append(line)
            continue

        key, _, value = line.partition("=")
        key = key.strip()

        if key == "AIRFLOW_UID" and hasattr(os, "getuid"):
            # Linux/macOS: container user must match the host user to own bind-mounted files.
            value = str(os.getuid())
        elif value.strip() == PLACEHOLDER:
            if key not in GENERATORS:
                sys.exit(f"No generator defined for placeholder key: {key}")
            value = GENERATORS[key]()
            generated[key] = value

        out_lines.append(f"{key}={value}")

    return "\n".join(out_lines) + "\n", generated


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--force", action="store_true", help="overwrite an existing .env")
    args = parser.parse_args()

    if not EXAMPLE.exists():
        sys.exit(f"Template not found: {EXAMPLE}")
    if TARGET.exists() and not args.force:
        sys.exit(".env already exists. Use --force to overwrite (this rotates all passwords).")

    rendered, generated = build_env(EXAMPLE.read_text(encoding="utf-8"))
    TARGET.write_text(rendered, encoding="utf-8")
    try:
        os.chmod(TARGET, 0o600)  # owner-only; silently ignored where unsupported (Windows)
    except OSError:
        pass

    admin_user = next(
        (line.split("=", 1)[1] for line in rendered.splitlines() if line.startswith("AIRFLOW_ADMIN_USER=")),
        "admin",
    )
    print(f".env created at {TARGET}")
    print(f"Airflow UI login -> user: {admin_user} | password: {generated.get('AIRFLOW_ADMIN_PASSWORD')}")
    print("Secrets live only in .env (git-ignored). Never paste them into commits, tickets or chats.")


if __name__ == "__main__":
    main()
