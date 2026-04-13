-- macros/parse_date.sql
-- Cross-platform date parsing: DuckDB (multi-format) vs Databricks (native cast).
--
-- DuckDB's cast(x as date) only handles YYYY-MM-DD. For robustness against
-- multiple date formats in CSV drops, we use try_strptime with a coalesce chain.
-- Databricks SQL handles multiple formats natively via cast(x as date).
--
-- Usage in bronze models:
--   {{ parse_date('date') }}

{% macro parse_date(column) %}
    {% if target.type == 'duckdb' %}
        cast(coalesce(
            try_strptime(cast({{ column }} as string), '%Y-%m-%d'),
            try_strptime(cast({{ column }} as string), '%d/%m/%Y'),
            try_strptime(cast({{ column }} as string), '%m-%d-%Y'),
            try_strptime(cast({{ column }} as string), '%Y-%m-%dT%H:%M:%S')
        ) as date)
    {% else %}
        cast({{ column }} as date)
    {% endif %}
{% endmacro %}
