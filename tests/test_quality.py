import unittest
from datetime import date

from sales_pipeline.quality import DataQualityError, assert_no_errors, run_stage_checks

D = date(2026, 10, 28)


class ScriptedConn:
    """Answers each SELECT by pattern, so we can simulate good and bad data."""

    def __init__(self, total=10, nulls=0, dups=0):
        self.answers = {"has_rows": total, "nulls": nulls, "dups": dups}

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def execute(self, sql, params=None):
        if "IS NULL" in sql:
            self._v = self.answers["nulls"]
        elif "HAVING" in sql:
            self._v = self.answers["dups"]
        else:
            self._v = self.answers["has_rows"]

    def fetchone(self):
        return (self._v,)


TABLES = {"sales": ("order_id", "product_id")}


class StageChecksTests(unittest.TestCase):
    def test_clean_data_passes(self):
        results = run_stage_checks(ScriptedConn(), D, TABLES)
        assert_no_errors(results)
        self.assertEqual(len(results), 1 + 2 + 1)  # rows + 2 key checks + duplicates

    def test_empty_batch_fails(self):
        with self.assertRaises(DataQualityError):
            assert_no_errors(run_stage_checks(ScriptedConn(total=0), D, TABLES))

    def test_null_keys_fail(self):
        with self.assertRaises(DataQualityError):
            assert_no_errors(run_stage_checks(ScriptedConn(nulls=3), D, TABLES))

    def test_duplicates_only_warn(self):
        results = run_stage_checks(ScriptedConn(dups=4), D, TABLES)
        assert_no_errors(results)  # must NOT raise
        warn = [r for r in results if r.severity == "warn"]
        self.assertEqual(warn[0].detail, "dup_keys=4")

    def test_unsafe_identifier_is_rejected(self):
        with self.assertRaises(ValueError):
            run_stage_checks(ScriptedConn(), D, {"sales; drop": ("x",)})


if __name__ == "__main__":
    unittest.main()
