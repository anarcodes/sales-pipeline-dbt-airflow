-- Manual idempotency check. Run it, run the DAG again for the SAME date, run it again: no change.
--   docker compose exec -T dwh-db psql -U dwh_user -d dwh -f - < sql/checks/row_counts.sql
SELECT 'stage.customers' AS layer_table, count(*)::text AS value FROM stage.customers
UNION ALL SELECT 'stage.products',  count(*)::text FROM stage.products
UNION ALL SELECT 'stage.sales',     count(*)::text FROM stage.sales
UNION ALL SELECT 'silver.customers', count(*)::text FROM silver.customers
UNION ALL SELECT 'silver.products',  count(*)::text FROM silver.products
UNION ALL SELECT 'silver.sales', count(*)::text || ' / net=' || coalesce(sum(net_amount), 0)::text FROM silver.sales
UNION ALL SELECT 'mart.d_customers', count(*)::text FROM mart.d_customers
UNION ALL SELECT 'mart.d_products',  count(*)::text FROM mart.d_products
UNION ALL SELECT 'mart.f_weekly_sales_snapshot',
       count(*)::text || ' / net=' || coalesce(sum(net_amount), 0)::text FROM mart.f_weekly_sales_snapshot
ORDER BY 1;
