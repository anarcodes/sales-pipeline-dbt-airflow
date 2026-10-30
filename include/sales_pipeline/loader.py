"""Idempotent batch loader for stage tables.

Contract: after `replace_batch(..., batch_date)` the table contains EXACTLY the given rows for that
batch date, no matter how many times it is called. Implementation: in ONE transaction

    DELETE rows of this batch  ->  INSERT the new rows  ->  count & reconcile  ->  COMMIT

Any failure (or a count mismatch) rolls everything back, so a half-loaded batch is impossible and
the previous good data of that batch date stays intact.

Only the DB-API is used (works with the psycopg2 connection from PostgresHook, and with fakes in
unit tests). Identifiers come from code constants and are still validated; values are always bound
parameters.
"""
from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from datetime import date
from typing import Any

logger = logging.getLogger(__name__)

BATCH_COLUMN = "_load_date"
INSERT_CHUNK_ROWS = 500
_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]*$")


class LoadReconciliationError(RuntimeError):
    """Rows present after the insert differ from rows we meant to insert."""


def _identifier(name: str) -> str:
    if not _IDENTIFIER.match(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def replace_batch(
    conn: Any,
    schema: str,
    table: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
    batch_date: date,
) -> int:
    """Replace all rows of `batch_date` in schema.table with `rows`. Returns rows loaded."""
    cols = [_identifier(c) for c in columns]
    target = f"{_identifier(schema)}.{_identifier(table)}"
    for row in rows:
        if len(row) != len(cols):
            raise ValueError(f"Row has {len(row)} values but {len(cols)} columns: {row!r}")

    column_list = ", ".join([*cols, BATCH_COLUMN])
    one_row = "(" + ", ".join(["%s"] * (len(cols) + 1)) + ")"

    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {target} WHERE {BATCH_COLUMN} = %s", (batch_date,))
            deleted = cur.rowcount
            for start in range(0, len(rows), INSERT_CHUNK_ROWS):
                chunk = rows[start : start + INSERT_CHUNK_ROWS]
                sql = (
                    f"INSERT INTO {target} ({column_list}) VALUES "
                    + ", ".join([one_row] * len(chunk))
                )
                cur.execute(sql, [value for row in chunk for value in (*row, batch_date)])
            cur.execute(f"SELECT count(*) FROM {target} WHERE {BATCH_COLUMN} = %s", (batch_date,))
            (loaded,) = cur.fetchone()
            if loaded != len(rows):
                raise LoadReconciliationError(
                    f"{target}: expected {len(rows)} rows for {batch_date}, found {loaded}"
                )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("Load of %s for %s failed; transaction rolled back", target, batch_date)
        raise

    logger.info("%s batch %s: replaced %s old rows with %s new rows",
                target, batch_date, deleted, len(rows))
    return len(rows)
