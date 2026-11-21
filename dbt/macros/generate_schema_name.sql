-- File: dbt/macros/generate_schema_name.sql
{# Use the custom schema name verbatim (ANALYTICS.STAGING / ANALYTICS.MARTS)
   instead of the default `<profile_schema>_<custom>` concatenation. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
