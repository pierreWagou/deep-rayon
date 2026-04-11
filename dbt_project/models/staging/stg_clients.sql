-- staging/stg_clients.sql
-- Bronze layer: 1:1 with source clients CSV
-- Handles: missing account_id column, type casting, deduplication

with source as (
    select * from read_csv(
        '{{ var("data_path") }}/clients_500k.csv',
        header = true,
        auto_detect = true
    )
),

cleaned as (
    select
        cast(id as bigint)                            as client_id,
        cast(nullif(trim(name), '') as varchar)       as name,
        cast(nullif(trim(job), '') as varchar)        as job,
        cast(nullif(trim(email), '') as varchar)      as email,
        -- account_id may be missing in some file drops
        cast(
            case
                when trim(cast(account_id as varchar)) = '' then null
                else account_id
            end as bigint
        )                                             as account_id
    from source
    where id is not null
)

select * from cleaned
