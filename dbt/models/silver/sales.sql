{#- Silver: typed order lines, one row per (order_id, product_id), amounts calculated once here. -#}
with source as (

    select * from {{ source('stage', 'sales') }}

),

cleaned as (

    select
        cast(cast(nullif(trim(order_id), '') as numeric) as integer)    as order_id,
        cast(cast(nullif(trim(product_id), '') as numeric) as integer)  as product_id,
        upper(nullif(trim(customer_id), ''))                            as customer_id,
        -- API sends ISO timestamps ("1997-01-15T00:00:00Z"); the business date is the first 10 chars
        cast(left(nullif(trim(order_date), ''), 10) as date)            as order_date,
        cast(nullif(trim(unit_price), '') as numeric(12, 2))            as unit_price,
        cast(cast(nullif(trim(quantity), '') as numeric) as integer)    as quantity,
        -- Discount arrives as a float32 (0.15000000596...). Round to a clean 2-decimal fraction.
        round(cast(coalesce(nullif(trim(discount), ''), '0') as numeric), 2) as discount,
        _load_date,
        _loaded_at
    from source

),

ranked as (

    select
        *,
        row_number() over (
            partition by order_id, product_id
            order by _load_date desc, _loaded_at desc, md5(cleaned::text)
        ) as rn
    from cleaned
    where order_id is not null
      and product_id is not null

),

deduplicated as (

    select * from ranked where rn = 1

)

select
    order_id,
    product_id,
    customer_id,
    order_date,
    unit_price,
    quantity,
    discount,
    round(unit_price * quantity, 2)                                  as gross_amount,
    round(unit_price * quantity * discount, 2)                       as discount_amount,
    round(unit_price * quantity, 2) - round(unit_price * quantity * discount, 2) as net_amount,
    _load_date as source_load_date
from deduplicated
