import unittest
from datetime import UTC, date, datetime
from types import SimpleNamespace

from sales_pipeline.dates import resolve_batch_date


class ResolveBatchDateTests(unittest.TestCase):
    def test_uses_logical_date_in_baku_timezone(self):
        run = SimpleNamespace(logical_date=datetime(2026, 10, 28, 5, 0, tzinfo=UTC))  # 09:00 Baku
        self.assertEqual(resolve_batch_date(run), date(2026, 10, 28))

    def test_timezone_matters_near_midnight(self):
        # 21:00 UTC on the 27th is already 01:00 on the 28th in Baku (UTC+4).
        run = SimpleNamespace(logical_date=datetime(2026, 10, 27, 21, 0, tzinfo=UTC))
        self.assertEqual(resolve_batch_date(run, "Asia/Baku"), date(2026, 10, 28))
        self.assertEqual(resolve_batch_date(run, "UTC"), date(2026, 10, 27))

    def test_falls_back_to_run_after_for_manual_runs(self):
        run = SimpleNamespace(logical_date=None, run_after=datetime(2026, 3, 1, 12, tzinfo=UTC))
        self.assertEqual(resolve_batch_date(run), date(2026, 3, 1))

    def test_falls_back_to_now_when_nothing_is_available(self):
        run = SimpleNamespace(logical_date=None, run_after=None)
        now = datetime(2026, 5, 5, 8, tzinfo=UTC)
        self.assertEqual(resolve_batch_date(run, now=now), date(2026, 5, 5))

    def test_naive_datetime_is_treated_as_utc(self):
        run = SimpleNamespace(logical_date=datetime(2026, 10, 27, 21, 0))
        self.assertEqual(resolve_batch_date(run), date(2026, 10, 28))


if __name__ == "__main__":
    unittest.main()
