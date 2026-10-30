{#-
    Fact (periodic snapshot). Grain: ONE row per  week x customer x product.
    week_start = Monday of the order's ISO week (Postgres date_trunc('week') starts on Monday).
    The date is cast to plain `timestamp` first: date_trunc on a `date` returns timestamptz, which
    would make the result depend on the session timezone.
    Rebuilt from silver on every run (table materialization) -> same input, same output (idempotent).
-#}
select
    cast(date_trunc('week', cast(order_date as timestamp)) as date) as week_start,
    {{ surrogate_key(['customer_id']) }}          as customer_key,
    {{ surrogate_key(['product_id']) }}           as product_key,
    sum(quantity)                                 as total_quantity,
    sum(gross_amount)                             as gross_amount,
    sum(discount_amount)                          as discount_amount,
    sum(net_amount)                               as net_amount,
    count(distinct order_id)                      as order_count,
    count(*)                                      as line_count
from {{ ref('sales') }}
where order_date is not null
group by 1, 2, 3
