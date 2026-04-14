-- tests/assert_avg_transactions_per_client_consistent.sql
-- Singular test: avg_transactions_per_client must equal
-- total_transactions / nb_clients (within rounding tolerance).
-- This validates the division logic in nb_clients_per_store.

select
    store_id,
    nb_clients,
    total_transactions,
    avg_transactions_per_client,
    round(total_transactions * 1.0 / nb_clients, 2) as expected
from {{ ref('nb_clients_per_store') }}
where
    abs(
        avg_transactions_per_client
        - round(total_transactions * 1.0 / nb_clients, 2)
    ) > 0.01
