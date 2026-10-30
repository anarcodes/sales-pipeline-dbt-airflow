#!/usr/bin/env python3
"""Prove idempotency end-to-end: run the whole DAG TWICE for the same date and compare every layer.

Usage (stack must be up:  docker compose up -d):
    python scripts/verify_idempotency.py                    # uses today's date
    python scripts/verify_idempotency.py --date 2026-10-28

What it does
  1. `airflow dags test sales_pipeline <date>`  (runs all tasks in-process, in order)
  2. fingerprint: row counts of stage (that batch), silver, mart + SUM(net_amount)
  3. run the DAG again for the SAME date, fingerprint again
  4. exit 0 if both fingerprints are identical, 1 otherwise
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

FINGERPRINT_SQL = """
SELECT 'stage.customers', count(*)::text FROM stage.customers WHERE _load_date = '{d}'
UNION ALL SELECT 'stage.products', count(*)::text FROM stage.products WHERE _load_date = '{d}'
UNION ALL SELECT 'stage.sales', count(*)::text FROM stage.sales WHERE _load_date = '{d}'
UNION ALL SELECT 'silver.customers', count(*)::text FROM silver.customers
UNION ALL SELECT 'silver.products', count(*)::text FROM silver.products
UNION ALL SELECT 'silver.sales', count(*)::text || ' / net=' || coalesce(sum(net_amount), 0)::text
       FROM silver.sales
UNION ALL SELECT 'mart.d_customers', count(*)::text FROM mart.d_customers
UNION ALL SELECT 'mart.d_products', count(*)::text FROM mart.d_products
UNION ALL SELECT 'mart.f_weekly_sales_snapshot',
       count(*)::text || ' / net=' || coalesce(sum(net_amount), 0)::text
       FROM mart.f_weekly_sales_snapshot
ORDER BY 1
"""


def read_env(path: Path) -> dict[str, str]:
    if not path.exists():
        sys.exit(".env not found. Run: python scripts/init_env.py")
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip()
    return env


def compose(*args: str) -> str:
    result = subprocess.run(
        ["docker", "compose", *args], cwd=ROOT, capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        sys.exit(f"`docker compose {' '.join(args)}` failed:\n{result.stderr or result.stdout}")
    return result.stdout


def fingerprint(env: dict[str, str], batch_date: str) -> dict[str, str]:
    out = compose(
        "exec", "-T", "dwh-db", "psql",
        "-U", env.get("DWH_USER", "dwh_user"), "-d", env.get("DWH_DB", "dwh"),
        "-t", "-A", "-F", "|", "-c", FINGERPRINT_SQL.format(d=batch_date),
    )
    result: dict[str, str] = {}
    for line in out.splitlines():
        if "|" in line:
            name, _, value = line.partition("|")
            result[name] = value
    return result


def run_dag(batch_date: str, label: str) -> bool:
    """Run the DAG in-process; keep the full output in logs/. Returns True if the command exited 0."""
    print(f"  running DAG for {batch_date} (this takes a few minutes) ...", flush=True)
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "airflow-scheduler",
         "airflow", "dags", "test", "sales_pipeline", batch_date],
        cwd=ROOT, capture_output=True, text=True, check=False,
    )
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / f"verify_{label}.log"
    log_file.write_text(result.stdout + "\n--- STDERR ---\n" + result.stderr, encoding="utf-8")
    print(f"  exit code {result.returncode}; full output: {log_file.relative_to(ROOT)}")
    return result.returncode == 0


def incomplete_tables(snapshot: dict[str, str]) -> list[str]:
    """Stage tables with no rows for the batch -> the run did not finish loading."""
    if not snapshot:
        return ["<no data returned>"]
    return [name for name, value in snapshot.items()
            if name.startswith("stage.") and value.split()[0] == "0"]


def run_until_complete(
    env: dict[str, str], batch_date: str, run_no: int, max_attempts: int = 3
) -> dict[str, str]:
    """`airflow dags test` does not retry failed tasks (the public API can be flaky), so we re-run.

    Re-running the same date is exactly what idempotency makes safe: a failed run is simply
    repeated until it completes. A failed run is NOT reported as an idempotency failure.
    """
    for attempt in range(1, max_attempts + 1):
        label = f"run{run_no}" if attempt == 1 else f"run{run_no}_attempt{attempt}"
        run_dag(batch_date, label)
        snapshot = fingerprint(env, batch_date)
        missing = incomplete_tables(snapshot)
        if not missing:
            return snapshot
        print(f"  attempt {attempt}/{max_attempts}: no stage rows in {missing} - run incomplete, repeating")
    sys.exit(
        f"Run {run_no} could not complete after {max_attempts} attempts (stage empty: {missing}).\n"
        "Open the newest logs/verify_run*.log and search for 'ERROR' / 'Traceback'. "
        "This is a source/pipeline failure, not an idempotency result."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the DAG twice for one date and compare.")
    parser.add_argument("--date", default=date.today().isoformat(), help="YYYY-MM-DD")
    args = parser.parse_args()
    date.fromisoformat(args.date)  # validate early
    env = read_env(ROOT / ".env")

    print("Run 1")
    first = run_until_complete(env, args.date, 1)
    print("Run 2 (same date)")
    second = run_until_complete(env, args.date, 2)

    width = max(map(len, first), default=10)
    print(f"\n{'table':<{width}}  {'run 1':<24} {'run 2':<24} result")
    ok = bool(first) and first.keys() == second.keys()
    for name in sorted(first):
        same = first[name] == second.get(name)
        ok = ok and same
        print(f"{name:<{width}}  {first[name]:<24} {second.get(name, '-'):<24} {'OK' if same else 'DIFF'}")
    print("\nIDEMPOTENT: yes" if ok else "\nIDEMPOTENT: NO - investigate")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
