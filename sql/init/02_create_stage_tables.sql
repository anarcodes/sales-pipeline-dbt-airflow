-- STAGE layer: 1 API entity = 1 table. Source: Northwind OData V4
--   Customers -> stage.customers | Products(+Category) -> stage.products | Orders(+Order_Details) -> stage.sales
--
-- Design rules:
--   * Business columns are TEXT. Stage never rejects or alters what the API sent;
--     type casting and cleaning are the silver layer's job (dbt).
--   * No primary key / unique constraint: duplicates must stay visible in raw data and are removed
--     deterministically in silver.
--   * _load_date = batch date (Airflow logical date in Asia/Baku, NOT now()).
--     Idempotency = "DELETE ... WHERE _load_date = :batch; INSERT ..." in ONE transaction.
--   * _loaded_at is audit-only (when rows physically landed); never used in business logic.

CREATE TABLE IF NOT EXISTS stage.customers (
    customer_id    text,
    company_name   text,
    contact_name   text,
    contact_title  text,
    address        text,
    city           text,
    region         text,
    postal_code    text,
    country        text,
    phone          text,
    _load_date     date        NOT NULL,
    _loaded_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS stage.products (
    product_id         text,
    product_name       text,
    supplier_id        text,
    category_id        text,
    category_name      text,
    quantity_per_unit  text,
    unit_price         text,
    units_in_stock     text,
    discontinued       text,
    _load_date         date        NOT NULL,
    _loaded_at         timestamptz NOT NULL DEFAULT now()
);

-- One row per order LINE (order x product).
CREATE TABLE IF NOT EXISTS stage.sales (
    order_id     text,
    product_id   text,
    customer_id  text,
    order_date   text,
    unit_price   text,
    quantity     text,
    discount     text,
    _load_date   date        NOT NULL,
    _loaded_at   timestamptz NOT NULL DEFAULT now()
);

-- The idempotent DELETE filters on _load_date, so index it.
CREATE INDEX IF NOT EXISTS ix_stage_customers_load_date ON stage.customers (_load_date);
CREATE INDEX IF NOT EXISTS ix_stage_products_load_date  ON stage.products  (_load_date);
CREATE INDEX IF NOT EXISTS ix_stage_sales_load_date     ON stage.sales     (_load_date);
