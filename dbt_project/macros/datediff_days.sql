-- macros/datediff_days.sql
-- Cross-platform day-level datediff: DuckDB requires quoted 'day',
-- Databricks requires unquoted DAY keyword.
--
-- Usage: {{ datediff_days('start_date', 'end_date') }}

{% macro datediff_days(start_date, end_date) %}
    {% if target.type == 'duckdb' %}
        datediff('day', {{ start_date }}, {{ end_date }})
    {% else %}
        datediff(DAY, {{ start_date }}, {{ end_date }})
    {% endif %}
{% endmacro %}
