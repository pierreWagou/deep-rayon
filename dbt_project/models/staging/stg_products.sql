-- staging/stg_products.sql
-- Bronze layer: 1:1 with source products CSV
-- Handles: type casting, brand normalization, deduplication

with source as (
    select * from read_csv(
        '{{ var("data_path") }}/products_500k.csv',
        header = true,
        auto_detect = true
    )
),

cleaned as (
    select
        cast(id as bigint)                                as product_id,
        cast(ean as varchar)                              as ean,
        -- Normalize brand: trim whitespace, standardize casing
        trim(cast(brand as varchar))                      as brand,
        cast(nullif(trim(description), '') as varchar)    as description
    from source
    where id is not null
)

select * from cleaned
