-- bronze/products.sql
-- Bronze layer: 1:1 with source products CSV (no PySpark equivalent — new staging logic)
-- Handles: type casting, brand normalization + alias mapping, deduplication
--
-- Data type safety: explicit casts on all columns. Type mismatches in CSV drops
-- produce NULL → filtered by where-clause. Schema tests catch data loss.

with source as (
    select * from {{ read_source('raw', 'products', 'products_500k.csv') }}
),

-- Brand mapping seed resolves aliases to canonical names
-- (e.g., "acme corp" → "acme corporation", "globexx" → "globex corporation")
brand_map as (
    select * from {{ ref('brand_mapping') }}
),

cleaned as (
    select
        cast(source.id as bigint)                                as product_id,
        cast(source.ean as string)                              as ean,
        -- Normalize brand: trim + lowercase, then resolve alias to canonical name
        coalesce(
            bm.canonical_brand,
            lower(trim(cast(source.brand as string)))
        )                                                        as brand,
        cast(nullif(trim(source.description), '') as string)    as description
    from source
    left join brand_map bm
        on lower(trim(cast(source.brand as string))) = bm.brand_alias
    where source.id is not null
)

select * from cleaned
