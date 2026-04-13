-- macros/read_source.sql
-- Dual-target source reader: DuckDB (local CSV) vs Databricks (CSV via read_files)
--
-- On DuckDB (dev):       read_csv() from the local data/ directory
-- On Databricks (prod):  read_files() from DBFS, Volumes, or Azure Blob Storage
--
-- The data_path variable controls the location on both targets:
--   Local:      "data" (default in dbt_project.yml)
--   Databricks: passed via --vars, e.g. "/FileStore/data" or "abfss://container@account.../data"
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
        read_files(
            '{{ var("data_path") }}/{{ csv_file }}',
            format => 'csv',
            header => 'true'
        )
    {% endif %}
{% endmacro %}
