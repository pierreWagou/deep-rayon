-- staging/stg_transactions.sql
-- Bronze layer: 1:1 with source transactions CSV
-- Handles: date format normalization, sign consistency, type casting

with source as (
    select * from read_csv(
        '{{ var("data_path") }}/transactions_500k.csv',
        header = true,
        auto_detect = true
    )
),

typed as (
    select
        cast(transaction_id as bigint)            as transaction_id,
        cast(client_id as bigint)                 as client_id,
        -- Normalize date: handle YYYY-MM-DD, DD/MM/YYYY, MM-DD-YYYY formats
        -- DuckDB's cast(... as date) handles YYYY-MM-DD natively
        -- For other formats, try_cast handles gracefully
        cast(date as date)                        as transaction_date,
        cast(hour as integer)                     as hour,
        cast(minute as integer)                   as minute,
        cast(product_id as bigint)                as product_id,
        cast(quantity as double)                  as raw_quantity,
        cast(spend as double)                     as raw_spend,
        cast(store_id as bigint)                  as store_id
    from source
    where transaction_id is not null
),

-- Fix sign consistency: quantity and spend must have the same sign
-- A purchase is positive, a return is negative
sign_corrected as (
    select
        transaction_id,
        client_id,
        transaction_date,
        hour,
        minute,
        product_id,
        -- If signs disagree, use quantity's sign as the source of truth
        raw_quantity                              as quantity,
        case
            when sign(raw_quantity) != sign(raw_spend) and raw_quantity != 0
            then abs(raw_spend) * sign(raw_quantity)
            else raw_spend
        end                                       as spend,
        store_id,
        -- Flag rows where sign correction was applied
        case
            when sign(raw_quantity) != sign(raw_spend) and raw_quantity != 0
            then true
            else false
        end                                       as is_sign_corrected
    from typed
    where raw_quantity != 0  -- exclude zero-quantity rows
)

select * from sign_corrected
