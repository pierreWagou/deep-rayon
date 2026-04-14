-- tests/assert_gold_stores_exist_in_bronze.sql
-- Singular test: every store_id in gold models must exist in the bronze stores table.
-- This catches orphaned store references caused by broken joins or stale data.

with gold_store_ids as (
    select distinct store_id from {{ ref('basket_analysis_per_store') }}
    union
    select distinct store_id from {{ ref('nb_clients_per_store') }}
    union
    select distinct store_id from {{ ref('product_trend_per_store') }}
),

bronze_stores as (
    select distinct store_id from {{ ref('stores') }}
)

select g.store_id
from gold_store_ids g
left join bronze_stores b on g.store_id = b.store_id
where b.store_id is null
