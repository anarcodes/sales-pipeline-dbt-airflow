{#- Silver: trimmed, blank->NULL, one row per customer (latest batch wins). -#}
with source as (

    select * from {{ source('stage', 'customers') }}

),

cleaned as (

    select
        upper(nullif(trim(customer_id), ''))   as customer_id,
        nullif(trim(company_name), '')         as company_name,
        nullif(trim(contact_name), '')         as contact_name,
        nullif(trim(contact_title), '')        as contact_title,
        nullif(trim(address), '')              as address,
        nullif(trim(city), '')                 as city,
        nullif(trim(region), '')               as region,
        nullif(trim(postal_code), '')          as postal_code,
        nullif(trim(country), '')              as country,
        nullif(trim(phone), '')                as phone,
        _load_date,
        _loaded_at
    from source

),

ranked as (

    select
        *,
        -- Deterministic: newest batch first, md5 of the row breaks exact ties -> same input, same output.
        row_number() over (
            partition by customer_id
            order by _load_date desc, _loaded_at desc, md5(cleaned::text)
        ) as rn
    from cleaned
    where customer_id is not null

)

select
    customer_id,
    company_name,
    contact_name,
    contact_title,
    address,
    city,
    region,
    postal_code,
    country,
    phone,
    _load_date as source_load_date
from ranked
where rn = 1
