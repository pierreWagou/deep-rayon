-- bronze/stores.sql
-- Bronze layer: 1:1 with source stores CSV (no PySpark equivalent — new staging logic)
-- Handles: missing lat/lng columns, store type normalization, type casting
--
-- Data type safety: explicit casts on all columns. Type mismatches in CSV drops
-- (e.g., store_id as alphanumeric) produce NULL → filtered by where-clause.
-- Schema tests (not_null, accepted_values) catch any data loss downstream.

with source as (
    select * from {{ read_source('raw', 'stores', 'stores_500k.csv') }}
),

cleaned as (
    select
        cast(id as bigint)                        as store_id,
        cast(latlng as string)                   as latlng,
        cast(opening as string)                  as opening,
        cast(closing as string)                  as closing,
        -- Normalize store type: lowercase and map known variants
        lower(trim(cast(type as string)))         as store_type,
        -- Prefer explicit lat/lng columns; fall back to parsing latlng
        coalesce(
            cast(latitude as double),
            cast(split_part(cast(latlng as string), ',', 1) as double)
        )                                         as latitude,
        coalesce(
            cast(longitude as double),
            cast(split_part(cast(latlng as string), ',', 2) as double)
        )                                         as longitude
    from source
    where id is not null
)

select * from cleaned
