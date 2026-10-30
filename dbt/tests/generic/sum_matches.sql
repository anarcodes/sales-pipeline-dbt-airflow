{#- Reconciliation: SUM(model.column) must equal SUM(compare_model.compare_column). Returns a row on mismatch. -#}
{% test sum_matches(model, column_name, compare_model, compare_column) %}

with model_total as (
    select coalesce(sum({{ column_name }}), 0) as total from {{ model }}
),

compare_total as (
    select coalesce(sum({{ compare_column }}), 0) as total from {{ compare_model }}
)

select
    model_total.total   as model_total,
    compare_total.total as compare_total
from model_total
cross join compare_total
where model_total.total <> compare_total.total

{% endtest %}
