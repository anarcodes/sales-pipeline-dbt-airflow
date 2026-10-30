{#- Passes when no combination of the given columns appears more than once (composite unique key). -#}
{% test unique_combination(model, combination_of_columns) %}

select
    {{ combination_of_columns | join(', ') }},
    count(*) as n_records
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1

{% endtest %}
