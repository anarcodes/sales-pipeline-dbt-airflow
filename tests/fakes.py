"""Test doubles: an in-memory 'database' that understands exactly the SQL our loader emits."""
from __future__ import annotations

from typing import Any


class FakeDB:
    """Stores rows per (table, batch_date); DELETE/INSERT/SELECT count work like Postgres would."""

    def __init__(self, n_columns: int) -> None:
        self.n_columns = n_columns  # business columns (without _load_date)
        self.rows: dict[str, list[tuple[Any, ...]]] = {}
        self.commits = 0
        self.rollbacks = 0
        self.statements: list[str] = []
        self.corrupt_insert = False  # simulate "fewer rows landed than we sent"

    def total(self, table: str, batch: Any) -> int:
        return len([r for r in self.rows.get(table, []) if r[-1] == batch])

    # --- DB-API surface used by loader ---
    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class FakeCursor:
    def __init__(self, db: FakeDB) -> None:
        self.db = db
        self.rowcount = 0
        self._result: tuple[Any, ...] | None = None

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        self.db.statements.append(sql.split("(")[0].strip())
        if sql.startswith("DELETE FROM"):
            table = sql.split()[2]
            before = self.db.rows.get(table, [])
            kept = [r for r in before if r[-1] != params[0]]
            self.rowcount = len(before) - len(kept)
            self.db.rows[table] = kept
        elif sql.startswith("INSERT INTO"):
            table = sql.split()[2]
            width = self.db.n_columns + 1
            new = [tuple(params[i : i + width]) for i in range(0, len(params), width)]
            if self.db.corrupt_insert:
                new = new[:-1]
            self.db.rows.setdefault(table, []).extend(new)
        elif sql.startswith("SELECT count(*)"):
            table = sql.split()[3]
            self._result = (self.db.total(table, params[0]),)
        else:  # pragma: no cover
            raise AssertionError(f"Unexpected SQL: {sql}")

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._result
