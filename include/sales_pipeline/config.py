"""Runtime settings, read from environment variables (nothing is hard-coded in the DAG)."""
from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

DEFAULT_BASE_URL = "https://services.odata.org/V4/Northwind/Northwind.svc"


@dataclass(frozen=True)
class Settings:
    source_base_url: str
    business_timezone: str
    http_connect_timeout: float
    http_read_timeout: float
    dwh_conn_id: str


def get_settings(env: Mapping[str, str] | None = None) -> Settings:
    env = os.environ if env is None else env
    return Settings(
        source_base_url=env.get("SOURCE_API_BASE_URL", DEFAULT_BASE_URL).rstrip("/"),
        business_timezone=env.get("BUSINESS_TIMEZONE", "Asia/Baku"),
        http_connect_timeout=float(env.get("SOURCE_API_CONNECT_TIMEOUT", "10")),
        http_read_timeout=float(env.get("SOURCE_API_READ_TIMEOUT", "120")),
        dwh_conn_id=env.get("DWH_CONN_ID", "dwh_postgres"),
    )
