import unittest
from datetime import date

from sales_pipeline.stage_load import ENTITIES, EmptyExtractError, load_entity
from sales_pipeline.transform import SALES_COLUMNS
from tests.fakes import FakeDB

D = date(2026, 10, 28)


class FakeClient:
    def __init__(self, records):
        self.records, self.calls = records, []

    def iter_records(self, entity_set, params=None):
        self.calls.append((entity_set, params))
        yield from self.records


ORDERS = [
    {"OrderID": 1, "CustomerID": "A", "OrderDate": "1997-01-01T00:00:00Z",
     "Order_Details": [{"ProductID": 1, "UnitPrice": 1, "Quantity": 1, "Discount": 0},
                       {"ProductID": 2, "UnitPrice": 2, "Quantity": 2, "Discount": 0.1}]},
    {"OrderID": 2, "CustomerID": "B", "OrderDate": "1997-01-02T00:00:00Z",
     "Order_Details": [{"ProductID": 1, "UnitPrice": 1, "Quantity": 5, "Discount": 0}]},
]


class LoadEntityTests(unittest.TestCase):
    def test_orders_are_flattened_to_one_row_per_line_and_load_is_idempotent(self):
        spec = ENTITIES["sales"]
        db, client = FakeDB(len(SALES_COLUMNS)), FakeClient(ORDERS)
        self.assertEqual(load_entity(spec, client, db, D), 3)
        self.assertEqual(load_entity(spec, client, db, D), 3)
        self.assertEqual(db.total("stage.sales", D), 3)
        self.assertEqual(client.calls[0][0], "Orders")

    def test_empty_extract_does_not_touch_the_database(self):
        db = FakeDB(len(SALES_COLUMNS))
        with self.assertRaises(EmptyExtractError):
            load_entity(ENTITIES["sales"], FakeClient([]), db, D)
        self.assertEqual(db.statements, [])

    def test_entity_definitions_are_consistent(self):
        self.assertEqual(set(ENTITIES), {"customers", "products", "sales"})
        for spec in ENTITIES.values():
            self.assertEqual(spec.name, spec.name.lower())
            self.assertTrue(spec.columns)


if __name__ == "__main__":
    unittest.main()
