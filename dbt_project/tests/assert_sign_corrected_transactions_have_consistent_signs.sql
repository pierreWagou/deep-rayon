-- tests/assert_sign_corrected_transactions_have_consistent_signs.sql
-- Singular test: after sign correction, all transactions should have
-- quantity and spend with the same sign direction

select
    transaction_id,
    quantity,
    spend,
    is_sign_corrected
from {{ ref('transactions_bronze') }}
where
    sign(quantity) != sign(spend)
    and quantity != 0
    and spend != 0
