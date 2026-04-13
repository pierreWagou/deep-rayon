-- tests/assert_rfm_scores_within_bounds.sql
-- Singular test: all RFM scores must be between 1 and 5

select
    client_id,
    recency_score,
    frequency_score,
    monetary_score
from {{ ref('customer') }}
where
    recency_score not between 1 and 5
    or frequency_score not between 1 and 5
    or monetary_score not between 1 and 5
