-- Singular test (silver): business-rule sanity. Any returned row is a failure.
select *
from {{ ref('sales') }}
where quantity <= 0
   or unit_price < 0
   or discount < 0
   or discount > 1
