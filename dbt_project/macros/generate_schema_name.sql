-- macros/generate_schema_name.sql
-- Override dbt's default schema naming to use clean layer names.
--
-- Default dbt behavior: {target_schema}_{custom_schema} → "default_bronze"
-- This override:        {custom_schema}                 → "bronze"
--
-- Result: schemas are simply "bronze", "silver", "gold" on all targets.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}
{%- endmacro %}
