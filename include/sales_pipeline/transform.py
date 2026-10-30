"""Map raw OData records to flat stage rows (all business values are text, as in stage tables).

No cleaning happens here on purpose: stage keeps what the API sent. Cleaning/typing = silver (dbt).
We only (a) flatten nested structures and (b) fail loudly if the API contract changed
(missing key fields = "schema drift").
"""
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

Row = tuple[str | None, ...]


class SchemaDriftError(ValueError):
    """The API response no longer matches the structure this pipeline expects."""


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):  # str(True) would be "True"; keep a stable lowercase form
        return "true" if value else "false"
    return str(value)


def _require(record: dict[str, Any], keys: Iterable[str], entity: str) -> None:
    missing = [key for key in keys if key not in record]
    if missing:
        raise SchemaDriftError(
            f"{entity}: missing field(s) {missing}; record keys: {sorted(record)}"
        )


# --- customers ---------------------------------------------------------------------------------
CUSTOMER_COLUMNS = (
    "customer_id", "company_name", "contact_name", "contact_title",
    "address", "city", "region", "postal_code", "country", "phone",
)
_CUSTOMER_FIELDS = (
    "CustomerID", "CompanyName", "ContactName", "ContactTitle",
    "Address", "City", "Region", "PostalCode", "Country", "Phone",
)


def customer_rows(record: dict[str, Any]) -> list[Row]:
    _require(record, ("CustomerID",), "Customers")
    return [tuple(_text(record.get(field)) for field in _CUSTOMER_FIELDS)]


# --- products (category name comes from $expand=Category) --------------------------------------
PRODUCT_COLUMNS = (
    "product_id", "product_name", "supplier_id", "category_id", "category_name",
    "quantity_per_unit", "unit_price", "units_in_stock", "discontinued",
)


def product_rows(record: dict[str, Any]) -> list[Row]:
    _require(record, ("ProductID",), "Products")
    category = record.get("Category") or {}
    return [
        (
            _text(record.get("ProductID")),
            _text(record.get("ProductName")),
            _text(record.get("SupplierID")),
            _text(record.get("CategoryID")),
            _text(category.get("CategoryName")),
            _text(record.get("QuantityPerUnit")),
            _text(record.get("UnitPrice")),
            _text(record.get("UnitsInStock")),
            _text(record.get("Discontinued")),
        )
    ]


# --- sales: one stage row per order LINE (Orders + $expand=Order_Details) ----------------------
SALES_COLUMNS = (
    "order_id", "product_id", "customer_id", "order_date", "unit_price", "quantity", "discount",
)


def sales_rows(order: dict[str, Any]) -> list[Row]:
    _require(order, ("OrderID", "Order_Details"), "Orders")
    rows: list[Row] = []
    for line in order["Order_Details"] or []:
        _require(line, ("ProductID", "UnitPrice", "Quantity"), "Order_Details")
        rows.append(
            (
                _text(order["OrderID"]),
                _text(line["ProductID"]),
                _text(order.get("CustomerID")),
                _text(order.get("OrderDate")),
                _text(line["UnitPrice"]),
                _text(line["Quantity"]),
                _text(line.get("Discount")),
            )
        )
    return rows
