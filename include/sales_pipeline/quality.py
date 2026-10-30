"""Data-quality gate for the stage layer (runs as an Airflow task after the loads).

error-level checks stop the pipeline; warn-level checks are logged (e.g. duplicates are expected
in raw data - silver removes them deterministically).
dbt tests then guard silver and mart.
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

# stage table -> business key columns
STAGE_KEYS: dict[str, tuple[str, ...]] = {
    "customers": ("customer_id",),
    "products": ("product_id",),
    "sales": ("order_id", "product_id"),
}
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str
    severity: str = "error"  # "error" | "warn"


def _safe(name: str) -> str:
    if not _IDENTIFIER.match(name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return name


def _scalar(cur: Any, sql: str, params: tuple[Any, ...]) -> int:
    cur.execute(sql, params)
    return int(cur.fetchone()[0])


def run_stage_checks(
    conn: Any, batch_date: date, tables: dict[str, tuple[str, ...]] | None = None
) -> list[CheckResult]:
    results: list[CheckResult] = []
    with conn.cursor() as cur:
        for table, keys in (tables or STAGE_KEYS).items():
            table = _safe(table)
            keys = tuple(_safe(k) for k in keys)
            where = "_load_date = %s"

            total = _scalar(cur, f"SELECT count(*) FROM stage.{table} WHERE {where}", (batch_date,))
            results.append(CheckResult(f"stage.{table}: batch has rows", total > 0, f"rows={total}"))

            for key in keys:
                nulls = _scalar(
                    cur,
                    f"SELECT count(*) FROM stage.{table} "
                    f"WHERE {where} AND ({key} IS NULL OR trim({key}) = '')",
                    (batch_date,),
                )
                results.append(
                    CheckResult(f"stage.{table}.{key}: no null/blank", nulls == 0, f"bad={nulls}")
                )

            key_list = ", ".join(keys)
            dups = _scalar(
                cur,
                f"SELECT count(*) FROM (SELECT 1 FROM stage.{table} WHERE {where} "
                f"GROUP BY {key_list} HAVING count(*) > 1) d",
                (batch_date,),
            )
            results.append(
                CheckResult(f"stage.{table}: duplicate keys", True, f"dup_keys={dups}", "warn")
            )
    return results


def assert_no_errors(results: list[CheckResult]) -> None:
    """Log every result; raise if any error-level check failed."""
    for result in results:
        level = logging.INFO if result.passed else (
            logging.WARNING if result.severity == "warn" else logging.ERROR
        )
        logger.log(level, "DQ [%s] %s - %s", "PASS" if result.passed else "FAIL",
                   result.name, result.detail)
    failed = [r for r in results if not r.passed and r.severity == "error"]
    if failed:
        raise DataQualityError("; ".join(f"{r.name} ({r.detail})" for r in failed))
