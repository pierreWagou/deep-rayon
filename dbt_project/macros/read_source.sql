-- macros/read_source.sql
-- Dual-target source reader: DuckDB (local CSV) vs Databricks (Unity Catalog)
--
-- On DuckDB (dev):  read_csv() from the local data/ directory
-- On Databricks (prod):  {{ source() }} referencing external tables in Unity Catalog
--
-- Usage in bronze models:
--   select * from {{ read_source('raw', 'transactions', 'transactions_500k.csv') }}

{% macro read_source(source_name, table_name, csv_file) %}
    {% if target.type == 'duckdb' %}
        read_csv(
            '{{ var("data_path") }}/{{ csv_file }}',
            header = true,
            auto_detect = true
        )
    {% else %}
        {{ source(source_name, table_name) }}
    {% endif %}
{% endmacro %}
