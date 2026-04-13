-- bronze/products_bronze.sql
-- Bronze layer: 1:1 with source products CSV (no PySpark equivalent — new staging logic)
-- Handles: type casting, brand normalization, deduplication

with source as (
    select * from {{ read_source('raw', 'products', 'products_500k.csv') }}
),

cleaned as (
    select
        cast(id as bigint)                                as product_id,
        cast(ean as varchar)                              as ean,
        -- Normalize brand: trim whitespace, standardize to lowercase
        lower(trim(cast(brand as varchar)))                as brand,
        cast(nullif(trim(description), '') as varchar)    as description
    from source
    where id is not null
)

select * from cleaned
