"""API -> stage: the three entity definitions and the generic extract+load routine."""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from typing import Any

from sales_pipeline.loader import replace_batch
from sales_pipeline.odata_client import ODataClient
from sales_pipeline.transform import (
    CUSTOMER_COLUMNS,
    PRODUCT_COLUMNS,
    SALES_COLUMNS,
    Row,
    customer_rows,
    product_rows,
    sales_rows,
)

logger = logging.getLogger(__name__)


class EmptyExtractError(RuntimeError):
    """The API returned (almost) nothing. We refuse to replace a good batch with an empty one."""


@dataclass(frozen=True)
class EntitySpec:
    name: str  # stage table name == silver source name
    entity_set: str  # OData entity set
    params: dict[str, str]
    columns: tuple[str, ...]
    to_rows: Callable[[dict[str, Any]], list[Row]]
    min_rows: int = 1


# $orderby gives a stable order for server-driven paging.
ENTITIES: dict[str, EntitySpec] = {
    "customers": EntitySpec(
        name="customers",
        entity_set="Customers",
        params={"$orderby": "CustomerID"},
        columns=CUSTOMER_COLUMNS,
        to_rows=customer_rows,
    ),
    "products": EntitySpec(
        name="products",
        entity_set="Products",
        params={"$expand": "Category", "$orderby": "ProductID"},
        columns=PRODUCT_COLUMNS,
        to_rows=product_rows,
    ),
    "sales": EntitySpec(
        name="sales",
        entity_set="Orders",
        params={"$expand": "Order_Details", "$orderby": "OrderID"},
        columns=SALES_COLUMNS,
        to_rows=sales_rows,
    ),
}


def load_entity(spec: EntitySpec, client: ODataClient, conn: Any, batch_date: date) -> int:
    """Extract one entity from the API and idempotently load it into stage.<name>."""
    logger.info("Extracting %s (batch %s)", spec.entity_set, batch_date)
    rows = [row for record in client.iter_records(spec.entity_set, spec.params)
            for row in spec.to_rows(record)]
    if len(rows) < spec.min_rows:
        raise EmptyExtractError(
            f"{spec.entity_set}: extracted {len(rows)} rows (< {spec.min_rows}); "
            "refusing to overwrite the batch"
        )
    return replace_batch(conn, "stage", spec.name, spec.columns, rows, batch_date)
