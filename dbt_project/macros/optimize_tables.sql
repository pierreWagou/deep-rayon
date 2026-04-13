-- macros/optimize_tables.sql
-- Macro to generate OPTIMIZE + Z-ORDER statements for Databricks
-- This is a no-op on DuckDB; only executed on Databricks target
--
-- Usage in a post-hook or standalone run:
--   {{ optimize_table('catalog.schema.table', ['col1', 'col2']) }}

{% macro optimize_table(table_name, z_order_columns) %}
    {% if target.type == 'databricks' %}
        OPTIMIZE {{ table_name }}
        {% if z_order_columns %}
            ZORDER BY ({{ z_order_columns | join(', ') }})
        {% endif %}
    {% else %}
        -- No-op on DuckDB: OPTIMIZE/ZORDER are Databricks-specific
        select 1 as _noop
    {% endif %}
{% endmacro %}


{% macro generate_optimization_statements() %}
{#
    Generates OPTIMIZE + Z-ORDER SQL for all configured tables.
    Run in production as a post-deployment step.

    Table optimization policy:
    ┌───────────────────────────────────┬─────────────────────────────────────────┬──────────────────────────────┐
    │ Table                             │ Z-ORDER columns                         │ Partitioning                 │
    ├───────────────────────────────────┼─────────────────────────────────────────┼──────────────────────────────┤
    │ customer_silver                   │ client_id, rfm_segment, customer_status │ None (500K rows)             │
    │ basket_analysis_per_store_gold    │ store_id, store_type                    │ None (aggregated, small)     │
    │ product_trend_per_store_gold     │ store_id, product_id, trend_direction   │ None (aggregated)            │
    │ nb_clients_per_store_gold        │ store_id, store_type                    │ None (aggregated, small)     │
    ├───────────────────────────────────┼─────────────────────────────────────────┼──────────────────────────────┤
    │ transactions_bronze (if materialized)│ transaction_date, client_id, store_id   │ PARTITION BY (transaction_date)│
    └───────────────────────────────────┴─────────────────────────────────────────┴──────────────────────────────┘

    Rationale:
    - Z-ORDER on filter/join columns reduces file skipping time
    - Transactions should be partitioned by date for time-range queries
    - Gold tables are small aggregates — Z-ORDER only, no partitioning
    - OPTIMIZE compacts small files from streaming/batch ingestion

    At scale (billions of rows):
    - Partition transactions by month (transaction_date truncated to month)
    - Consider liquid clustering (Databricks) as a replacement for Z-ORDER
    - Schedule OPTIMIZE weekly, not on every run
    - Use VACUUM to clean up old files after OPTIMIZE
#}

{% if target.type == 'databricks' %}
    -- Silver layer
    OPTIMIZE {{ target.catalog }}.{{ target.schema }}_silver.customer_silver
    ZORDER BY (client_id, rfm_segment, customer_status);

    -- Gold layer
    OPTIMIZE {{ target.catalog }}.{{ target.schema }}_gold.basket_analysis_per_store_gold
    ZORDER BY (store_id, store_type);

    OPTIMIZE {{ target.catalog }}.{{ target.schema }}_gold.product_trend_per_store_gold
    ZORDER BY (store_id, product_id, trend_direction);

    OPTIMIZE {{ target.catalog }}.{{ target.schema }}_gold.nb_clients_per_store_gold
    ZORDER BY (store_id, store_type);
{% endif %}
{% endmacro %}
