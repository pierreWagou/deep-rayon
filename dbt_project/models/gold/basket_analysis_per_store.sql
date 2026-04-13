-- gold/basket_analysis_per_store.sql
-- Gold layer: Basket analysis KPIs aggregated by store
-- Translated from: reference/pipeline/gold_datamart_kpis.py (lines 152-202)
--
-- Computes per-store metrics:
--   - Average, min, max, stddev basket size
--   - Average basket item count
--   - Total transactions
--   - Store metadata (type, location, hours)

with transactions as (
    select * from {{ ref('transactions') }}
),

stores as (
    select * from {{ ref('stores') }}
),

-- Per-basket aggregation: group by store + transaction
baskets as (
    select
        store_id,
        transaction_id,
        transaction_date,
        sum(quantity)                as basket_size,
        count(distinct product_id)   as basket_item_count
    from transactions
    group by store_id, transaction_id, transaction_date
),

-- Per-store aggregation
store_metrics as (
    select
        store_id,
        round(avg(basket_size), 2)                  as avg_basket_size,
        round(avg(basket_item_count), 2)            as avg_basket_item_count,
        round(stddev_samp(basket_size), 2)          as stddev_basket_size,
        min(basket_size)                            as min_basket_size,
        max(basket_size)                            as max_basket_size,
        count(*)                                    as total_transactions
    from baskets
    group by store_id
),

-- Enrich with store metadata
final as (
    select
        sm.store_id,
        sm.avg_basket_size,
        sm.avg_basket_item_count,
        sm.stddev_basket_size,
        sm.min_basket_size,
        sm.max_basket_size,
        sm.total_transactions,
        s.store_type,
        s.latitude,
        s.longitude,
        s.opening,
        s.closing,
        current_timestamp as created_at
    from store_metrics sm
    left join stores s on sm.store_id = s.store_id
)

select * from final
