import unittest

from sales_pipeline import transform as t


class CustomerTests(unittest.TestCase):
    def test_maps_fields_in_column_order_and_keeps_nulls(self):
        rec = {"CustomerID": "ALFKI", "CompanyName": "Alfreds", "Region": None, "Country": "Germany"}
        row = t.customer_rows(rec)[0]
        self.assertEqual(len(row), len(t.CUSTOMER_COLUMNS))
        self.assertEqual(row[0], "ALFKI")
        self.assertIsNone(row[t.CUSTOMER_COLUMNS.index("region")])
        self.assertEqual(row[t.CUSTOMER_COLUMNS.index("country")], "Germany")

    def test_missing_key_is_schema_drift(self):
        with self.assertRaises(t.SchemaDriftError):
            t.customer_rows({"CompanyName": "x"})


class ProductTests(unittest.TestCase):
    def test_category_name_is_flattened_and_bool_is_lowercase_text(self):
        rec = {"ProductID": 1, "ProductName": "Chai", "UnitPrice": 18, "Discontinued": False,
               "Category": {"CategoryID": 1, "CategoryName": "Beverages", "Picture": "xxx"}}
        row = t.product_rows(rec)[0]
        self.assertEqual(len(row), len(t.PRODUCT_COLUMNS))
        self.assertEqual(row[t.PRODUCT_COLUMNS.index("category_name")], "Beverages")
        self.assertEqual(row[t.PRODUCT_COLUMNS.index("discontinued")], "false")
        self.assertEqual(row[t.PRODUCT_COLUMNS.index("unit_price")], "18")

    def test_product_without_category_is_allowed(self):
        row = t.product_rows({"ProductID": 2, "Category": None})[0]
        self.assertIsNone(row[t.PRODUCT_COLUMNS.index("category_name")])


class SalesTests(unittest.TestCase):
    ORDER = {
        "OrderID": 10248, "CustomerID": "VINET", "OrderDate": "1996-07-04T00:00:00Z",
        "Order_Details": [
            {"ProductID": 11, "UnitPrice": 14.0, "Quantity": 12, "Discount": 0},
            {"ProductID": 42, "UnitPrice": 9.8, "Quantity": 10, "Discount": 0.15000000596046448},
        ],
    }

    def test_one_row_per_order_line_with_order_fields_repeated(self):
        rows = t.sales_rows(self.ORDER)
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertEqual(len(row), len(t.SALES_COLUMNS))
            self.assertEqual(row[t.SALES_COLUMNS.index("order_id")], "10248")
            self.assertEqual(row[t.SALES_COLUMNS.index("customer_id")], "VINET")
            self.assertEqual(row[t.SALES_COLUMNS.index("order_date")], "1996-07-04T00:00:00Z")
        self.assertEqual(rows[1][t.SALES_COLUMNS.index("product_id")], "42")

    def test_order_without_lines_yields_no_rows(self):
        self.assertEqual(t.sales_rows({"OrderID": 1, "Order_Details": []}), [])

    def test_missing_nested_collection_is_schema_drift(self):
        with self.assertRaises(t.SchemaDriftError):
            t.sales_rows({"OrderID": 1})

    def test_missing_line_field_is_schema_drift(self):
        with self.assertRaises(t.SchemaDriftError):
            t.sales_rows({"OrderID": 1, "Order_Details": [{"ProductID": 1, "Quantity": 2}]})


if __name__ == "__main__":
    unittest.main()
