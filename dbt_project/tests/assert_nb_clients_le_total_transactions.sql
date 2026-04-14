-- tests/assert_nb_clients_le_total_transactions.sql
-- Singular test: a store cannot have more unique clients than total transactions.
-- Each client must have at least one transaction, so nb_clients <= total_transactions.

select
    store_id,
    nb_clients,
    total_transactions
from {{ ref('nb_clients_per_store') }}
where
    nb_clients > total_transactions
