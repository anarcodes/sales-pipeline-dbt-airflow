{#-
    dbt's default builds "<target_schema>_<custom_schema>" (e.g. silver_mart). We want the schema
    names EXACTLY as configured (silver / mart), as the task requires.
-#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
