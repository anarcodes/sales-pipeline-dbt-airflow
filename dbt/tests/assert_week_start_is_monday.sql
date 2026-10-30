-- Singular test (mart): every week_start must be a Monday (ISO day-of-week 1).
select *
from {{ ref('f_weekly_sales_snapshot') }}
where extract(isodow from week_start) <> 1
