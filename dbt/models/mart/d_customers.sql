{#- Dimension: one row per customer, keyed by customer_key. -#}
select
    {{ surrogate_key(['customer_id']) }} as customer_key,
    customer_id,
    company_name,
    contact_name,
    contact_title,
    city,
    region,
    country,
    postal_code,
    phone
from {{ ref('customers') }}
