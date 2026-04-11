-- gold/product_trend_per_store.sql
-- Gold layer: Product sales trends per store with 30/60/90-day windows
-- Translated from: DataEngineeringTest/pipeline/gold_datamart_kpis.py (lines 206-283)
-- NOTE: Fixed bug from PySpark reference (line 265) where join used
--       store_id == product_id instead of store_id == stores.id
--
-- Computes per store-product metrics:
--   - Sales in 30d, 60d, 90d windows
--   - Trend direction (Increasing/Decreasing/Stable)
--   - Sales velocity
--   - Product and store metadata

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

stores as (
    select * from {{ ref('stg_stores') }}
),

-- Add period flags based on transaction date relative to current date
with_periods as (
    select
        store_id,
        product_id,
        quantity,
        transaction_date,
        case when datediff('day', transaction_date, current_date) <= 30
             then quantity else 0 end as sales_30d,
        case when datediff('day', transaction_date, current_date) <= 60
             then quantity else 0 end as sales_60d,
        case when datediff('day', transaction_date, current_date) <= 90
             then quantity else 0 end as sales_90d
    from transactions
),

-- Aggregate by store + product
store_product_metrics as (
    select
        store_id,
        product_id,
        sum(sales_30d)                          as sales_30d,
        sum(sales_60d)                          as sales_60d,
        sum(sales_90d)                          as sales_90d,
        sum(quantity)                            as total_sales_all_time,
        count(*)                                as transaction_count
    from with_periods
    group by store_id, product_id
),

-- Compute trend indicators
with_trends as (
    select
        *,
        -- Sales in the 30-60d window (for comparison)
        sales_60d - sales_30d                   as sales_30d_to_60d,
        -- Trend direction: compare last 30d to the prior 30d
        case
            when (sales_60d - sales_30d) = 0 then 'Stable'
            when sales_30d > (sales_60d - sales_30d) then 'Increasing'
            when sales_30d < (sales_60d - sales_30d) then 'Decreasing'
            else 'Stable'
        end                                     as trend_direction,
        -- Percentage change (30d vs prior 30d)
        case
            when (sales_60d - sales_30d) = 0 then 0.0
            else round(
                (sales_30d - (sales_60d - sales_30d)) * 100.0
                / nullif(sales_60d - sales_30d, 0),
                2
            )
        end                                     as trend_30d_vs_60d_pct,
        -- Sales velocity (units per day in last 30 days)
        round(sales_30d / 30.0, 4)              as sales_velocity_30d
    from store_product_metrics
),

-- Enrich with product and store metadata
-- BUG FIX: original PySpark joined stores on store_id == product_id
final as (
    select
        wt.store_id,
        wt.product_id,
        wt.sales_30d,
        wt.sales_60d,
        wt.sales_90d,
        wt.total_sales_all_time,
        wt.transaction_count,
        wt.trend_direction,
        wt.trend_30d_vs_60d_pct,
        wt.sales_velocity_30d,
        p.brand,
        p.ean,
        s.store_type,
        current_timestamp as created_at
    from with_trends wt
    left join products p on wt.product_id = p.product_id
    left join stores s on wt.store_id = s.store_id
)

select * from final
