-- staging/stg_stores.sql
-- Bronze layer: 1:1 with source stores CSV
-- Handles: missing lat/lng columns, store type normalization, type casting

with source as (
    select * from read_csv(
        '{{ var("data_path") }}/stores_500k.csv',
        header = true,
        auto_detect = true
    )
),

cleaned as (
    select
        cast(id as bigint)                        as store_id,
        cast(latlng as varchar)                   as latlng,
        cast(opening as varchar)                  as opening,
        cast(closing as varchar)                  as closing,
        -- Normalize store type: lowercase and map known variants
        lower(trim(cast(type as varchar)))         as store_type,
        -- Prefer explicit lat/lng columns; fall back to parsing latlng
        coalesce(
            cast(latitude as double),
            cast(split_part(cast(latlng as varchar), ',', 1) as double)
        )                                         as latitude,
        coalesce(
            cast(longitude as double),
            cast(split_part(cast(latlng as varchar), ',', 2) as double)
        )                                         as longitude
    from source
    where id is not null
)

select * from cleaned
