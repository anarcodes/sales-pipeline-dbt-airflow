import unittest
from datetime import date

from sales_pipeline.loader import INSERT_CHUNK_ROWS, LoadReconciliationError, replace_batch
from tests.fakes import FakeDB

D1, D2 = date(2026, 10, 27), date(2026, 10, 28)
COLS = ("a", "b")


def rows(n, tag="x"):
    return [(f"{tag}{i}", f"v{i}") for i in range(n)]


class ReplaceBatchTests(unittest.TestCase):
    def test_rerunning_the_same_batch_is_idempotent(self):
        db = FakeDB(len(COLS))
        replace_batch(db, "stage", "t", COLS, rows(10), D1)
        replace_batch(db, "stage", "t", COLS, rows(10), D1)
        replace_batch(db, "stage", "t", COLS, rows(10), D1)
        self.assertEqual(db.total("stage.t", D1), 10)  # NOT 30

    def test_rerun_replaces_content_not_just_count(self):
        db = FakeDB(len(COLS))
        replace_batch(db, "stage", "t", COLS, rows(3, "old"), D1)
        replace_batch(db, "stage", "t", COLS, rows(3, "new"), D1)
        self.assertTrue(all(r[0].startswith("new") for r in db.rows["stage.t"]))

    def test_other_batches_are_untouched(self):
        db = FakeDB(len(COLS))
        replace_batch(db, "stage", "t", COLS, rows(5), D1)
        replace_batch(db, "stage", "t", COLS, rows(7), D2)
        replace_batch(db, "stage", "t", COLS, rows(5), D1)
        self.assertEqual((db.total("stage.t", D1), db.total("stage.t", D2)), (5, 7))

    def test_statement_order_is_delete_then_insert_then_count_then_commit(self):
        db = FakeDB(len(COLS))
        replace_batch(db, "stage", "t", COLS, rows(2), D1)
        self.assertEqual(db.statements[0], "DELETE FROM stage.t WHERE _load_date = %s")
        self.assertTrue(db.statements[1].startswith("INSERT INTO stage.t"))
        self.assertTrue(db.statements[-1].startswith("SELECT count"))
        self.assertEqual((db.commits, db.rollbacks), (1, 0))

    def test_large_loads_are_chunked(self):
        db = FakeDB(len(COLS))
        n = INSERT_CHUNK_ROWS * 2 + 1
        replace_batch(db, "stage", "t", COLS, rows(n), D1)
        inserts = [s for s in db.statements if s.startswith("INSERT")]
        self.assertEqual(len(inserts), 3)
        self.assertEqual(db.total("stage.t", D1), n)

    def test_count_mismatch_rolls_back_and_raises(self):
        db = FakeDB(len(COLS))
        db.corrupt_insert = True
        with self.assertRaises(LoadReconciliationError):
            replace_batch(db, "stage", "t", COLS, rows(5), D1)
        self.assertEqual((db.commits, db.rollbacks), (0, 1))

    def test_unsafe_identifiers_are_rejected(self):
        db = FakeDB(1)
        with self.assertRaises(ValueError):
            replace_batch(db, "stage", "t; DROP TABLE x", ("a",), [("1",)], D1)
        with self.assertRaises(ValueError):
            replace_batch(db, "stage", "t", ("a b",), [("1",)], D1)

    def test_row_width_mismatch_is_rejected_before_touching_the_db(self):
        db = FakeDB(len(COLS))
        with self.assertRaises(ValueError):
            replace_batch(db, "stage", "t", COLS, [("only-one",)], D1)
        self.assertEqual(db.statements, [])


if __name__ == "__main__":
    unittest.main()
