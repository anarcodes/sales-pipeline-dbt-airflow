{#-
    Deterministic surrogate key: md5 of the natural key. Same input -> same key on every run
    (a row_number()-based key would shift when rows are added). Dimension and fact derive keys with
    this same macro, so they always join.
-#}
{% macro surrogate_key(columns) -%}
    md5(
    {%- for column in columns -%}
        coalesce(cast({{ column }} as text), '_null_')
        {%- if not loop.last %} || '|' || {% endif -%}
    {%- endfor -%}
    )
{%- endmacro %}
