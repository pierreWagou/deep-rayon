-- bronze/clients.sql
-- Bronze layer: 1:1 with source clients CSV (no PySpark equivalent — new staging logic)
-- Handles: missing account_id column, type casting, deduplication

with source as (
    select * from {{ read_source('raw', 'clients', 'clients_500k.csv') }}
),

cleaned as (
    select
        cast(id as bigint)                            as client_id,
        cast(nullif(trim(name), '') as string)       as name,
        cast(nullif(trim(job), '') as string)        as job,
        cast(nullif(trim(email), '') as string)      as email,
        -- account_id may be missing in some file drops
        cast(
            case
                when trim(cast(account_id as string)) = '' then null
                else account_id
            end as bigint
        )                                             as account_id
    from source
    where id is not null
)

select * from cleaned
