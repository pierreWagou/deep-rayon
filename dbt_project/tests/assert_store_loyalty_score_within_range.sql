-- tests/assert_store_loyalty_score_within_range.sql
-- Singular test: store loyalty score must be between 0 and 100

select
    client_id,
    store_loyalty_score
from {{ ref('customer_silver') }}
where
    store_loyalty_score < 0
    or store_loyalty_score > 100
