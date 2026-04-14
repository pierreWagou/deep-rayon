-- tests/assert_basket_min_le_max.sql
-- Singular test: min_basket_size must never exceed max_basket_size

select
    store_id,
    min_basket_size,
    max_basket_size
from {{ ref('basket_analysis_per_store') }}
where
    min_basket_size > max_basket_size
