{#- Dimension: one row per product, keyed by product_key. -#}
select
    {{ surrogate_key(['product_id']) }} as product_key,
    product_id,
    product_name,
    category_name,
    quantity_per_unit,
    unit_price as list_price,
    is_discontinued
from {{ ref('products') }}
