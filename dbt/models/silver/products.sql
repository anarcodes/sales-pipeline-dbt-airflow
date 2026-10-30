{#- Silver: typed (numeric/integer/boolean), one row per product (latest batch wins). -#}
with source as (

    select * from {{ source('stage', 'products') }}

),

cleaned as (

    select
        cast(cast(nullif(trim(product_id), '') as numeric) as integer)      as product_id,
        nullif(trim(product_name), '')                                      as product_name,
        cast(cast(nullif(trim(supplier_id), '') as numeric) as integer)     as supplier_id,
        cast(cast(nullif(trim(category_id), '') as numeric) as integer)     as category_id,
        nullif(trim(category_name), '')                                     as category_name,
        nullif(trim(quantity_per_unit), '')                                 as quantity_per_unit,
        cast(nullif(trim(unit_price), '') as numeric(12, 2))                as unit_price,
        cast(cast(nullif(trim(units_in_stock), '') as numeric) as integer)  as units_in_stock,
        lower(trim(discontinued)) in ('true', 't', '1', 'yes')              as is_discontinued,
        _load_date,
        _loaded_at
    from source

),

ranked as (

    select
        *,
        row_number() over (
            partition by product_id
            order by _load_date desc, _loaded_at desc, md5(cleaned::text)
        ) as rn
    from cleaned
    where product_id is not null

)

select
    product_id,
    product_name,
    supplier_id,
    category_id,
    category_name,
    quantity_per_unit,
    unit_price,
    units_in_stock,
    is_discontinued,
    _load_date as source_load_date
from ranked
where rn = 1
