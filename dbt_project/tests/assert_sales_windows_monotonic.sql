-- tests/assert_sales_windows_monotonic.sql
-- Singular test: sales window columns must be monotonically non-decreasing
-- (sales_30d <= sales_60d <= sales_90d <= total_sales_all_time)
-- because each wider window is a superset of the narrower one.

select
    store_id,
    product_id,
    sales_30d,
    sales_60d,
    sales_90d,
    total_sales_all_time
from {{ ref('product_trend_per_store') }}
where
    sales_30d > sales_60d
    or sales_60d > sales_90d
    or sales_90d > total_sales_all_time
