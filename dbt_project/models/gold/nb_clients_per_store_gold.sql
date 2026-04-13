-- gold/nb_clients_per_store_gold.sql
-- Gold layer: Client count and engagement metrics per store
-- Translated from: reference/pipeline/gold_datamart_kpis.py (lines 287-322)
--
-- Computes per-store metrics:
--   - Number of distinct clients
--   - Total transactions
--   - Total and average quantity
--   - Average transactions per client
--   - Store metadata

with transactions as (
    select * from {{ ref('transactions_bronze') }}
),

stores as (
    select * from {{ ref('stores_bronze') }}
),

-- Aggregate by store
store_client_metrics as (
    select
        store_id,
        count(distinct client_id)                       as nb_clients,
        count(distinct transaction_id)                   as total_transactions,
        sum(quantity)                                    as total_quantity,
        round(avg(quantity), 2)                          as avg_quantity,
        round(
            count(distinct transaction_id) * 1.0
            / nullif(count(distinct client_id), 0),
            2
        )                                               as avg_transactions_per_client
    from transactions
    group by store_id
),

-- Enrich with store metadata
final as (
    select
        scm.store_id,
        scm.nb_clients,
        scm.total_transactions,
        scm.total_quantity,
        scm.avg_quantity,
        scm.avg_transactions_per_client,
        s.store_type,
        s.latitude,
        s.longitude,
        s.opening,
        s.closing,
        current_timestamp as created_at
    from store_client_metrics scm
    left join stores s on scm.store_id = s.store_id
)

select * from final
