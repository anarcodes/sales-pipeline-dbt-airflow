-- Executed ONCE by the postgres image, the first time the dwh-db volume is empty
-- (/docker-entrypoint-initdb.d). To re-run:  docker compose down -v  (this deletes the data!)
--
-- Layers:  stage (raw API data)  ->  silver (clean, typed, deduplicated; dbt)  ->  mart (star schema; dbt)

CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS mart;

COMMENT ON SCHEMA stage  IS 'Raw data exactly as delivered by the APIs (all business columns are text). Loaded by Airflow.';
COMMENT ON SCHEMA silver IS 'Cleaned, typed, deduplicated data. Built by dbt.';
COMMENT ON SCHEMA mart   IS 'Star schema for reporting: d_customers, d_products, f_weekly_sales_snapshot. Built by dbt.';
