-- silver/customer_silver.sql
-- Silver layer: Customer analytics with RFM scoring and segmentation
-- Translated from: DataEngineeringTest/pipeline/silver_customer_layer.py
--
-- This model computes:
--   1. RFM metrics (Recency, Frequency, Monetary)
--   2. RFM scores (1-5 scale)
--   3. RFM segmentation (Champion, Loyal, At Risk, etc.)
--   4. Customer status and lifecycle stage
--   5. Primary store preference and loyalty score

with transactions as (
    select * from {{ ref('stg_transactions') }}
),

clients as (
    select * from {{ ref('stg_clients') }}
),

stores as (
    select * from {{ ref('stg_stores') }}
),

-- Step 1: RFM Metrics
rfm_metrics as (
    select
        client_id,
        datediff('day', max(transaction_date), current_date)   as recency_days,
        count(distinct transaction_id)                          as frequency,
        sum(quantity)                                           as monetary_value,
        min(transaction_date)                                   as first_purchase_date,
        max(transaction_date)                                   as last_purchase_date,
        round(avg(quantity), 2)                                 as avg_quantity_per_transaction,
        count(distinct transaction_id)                          as total_transactions
    from transactions
    group by client_id
),

-- Step 2: RFM Scores (1-5 scale, matching PySpark thresholds)
rfm_scored as (
    select
        *,
        -- Recency score: lower recency = higher score
        case
            when recency_days <= 30  then 5
            when recency_days <= 60  then 4
            when recency_days <= 90  then 3
            when recency_days <= 180 then 2
            else 1
        end as recency_score,
        -- Frequency score: higher frequency = higher score
        case
            when frequency >= 20 then 5
            when frequency >= 10 then 4
            when frequency >= 5  then 3
            when frequency >= 2  then 2
            else 1
        end as frequency_score,
        -- Monetary score: higher monetary = higher score
        case
            when monetary_value >= 100 then 5
            when monetary_value >= 50  then 4
            when monetary_value >= 20  then 3
            when monetary_value >= 5   then 2
            else 1
        end as monetary_score
    from rfm_metrics
),

-- Step 3: RFM Segmentation
rfm_segmented as (
    select
        *,
        case
            when recency_score >= 4 and frequency_score >= 4 and monetary_score >= 4
                then 'Champion'
            when recency_score >= 3 and frequency_score >= 3 and monetary_score >= 3
                then 'Loyal Customer'
            when recency_score <= 2 and frequency_score >= 3 and monetary_score >= 3
                then 'At Risk'
            when recency_score <= 2 and frequency_score <= 2 and monetary_score <= 2
                then 'Lost'
            when recency_score <= 2 and frequency_score >= 2
                then 'Hibernating'
            when recency_score >= 3 and frequency_score <= 2
                then 'Need Attention'
            when recency_score >= 2 and recency_score <= 3 and frequency_score <= 2
                then 'About to Sleep'
            else 'Potential Loyalist'
        end as rfm_segment
    from rfm_scored
),

-- Step 4: Customer Status and Lifecycle
customer_status as (
    select
        *,
        case
            when recency_days <= 30  then 'Active'
            when recency_days <= 90  then 'Inactive'
            else 'Churned'
        end as customer_status,
        case
            when frequency = 1 then 'New'
            when recency_days <= 30  then 'Active'
            when recency_days <= 90  then 'Lapsed'
            else 'Churned'
        end as customer_lifecycle_stage,
        frequency > 1 as is_repeat_customer
    from rfm_segmented
),

-- Step 5: Primary Store Preference
store_visits as (
    select
        client_id,
        store_id,
        count(*)          as transaction_count,
        sum(quantity)      as total_quantity
    from transactions
    group by client_id, store_id
),

ranked_stores as (
    select
        *,
        row_number() over (
            partition by client_id
            order by transaction_count desc, total_quantity desc
        ) as store_rank
    from store_visits
),

primary_store as (
    select
        rs.client_id,
        rs.store_id                                           as primary_store_id,
        s.store_type                                          as primary_store_type,
        rs.transaction_count                                  as primary_store_transaction_count,
        round(
            rs.transaction_count * 100.0 / sum(rs.transaction_count) over (partition by rs.client_id),
            2
        )                                                     as store_loyalty_score
    from ranked_stores rs
    left join stores s on rs.store_id = s.store_id
    where rs.store_rank = 1
),

-- Final join: combine all dimensions
final as (
    select
        cs.client_id,
        c.name                                as client_name,
        c.job                                 as client_job,
        c.email                               as client_email,
        c.account_id,
        cs.recency_days,
        cs.frequency,
        cs.monetary_value,
        cs.recency_score,
        cs.frequency_score,
        cs.monetary_score,
        cs.rfm_segment,
        cs.customer_status,
        cs.customer_lifecycle_stage,
        cs.is_repeat_customer,
        cs.first_purchase_date,
        cs.last_purchase_date,
        cs.avg_quantity_per_transaction,
        ps.primary_store_id,
        ps.primary_store_type,
        ps.primary_store_transaction_count,
        ps.store_loyalty_score,
        cs.total_transactions,
        current_timestamp                     as created_at,
        current_timestamp                     as updated_at
    from customer_status cs
    left join clients c on cs.client_id = c.client_id
    left join primary_store ps on cs.client_id = ps.client_id
)

select * from final
