# Original PySpark Pipeline (Read-Only Reference)

This directory contains the original PySpark pipeline and test instructions
from the `DataEngineeringTest` repository. These files represent the **"before"
state** that was migrated to dbt.

They are preserved for:

- **Traceability** -- every dbt model header comment references its PySpark
  origin with file name and line numbers.
- **Reviewer context** -- a reviewer can compare the PySpark logic against
  the dbt SQL to verify the translation.
- **Bug documentation** -- the known join bug on line 265 of
  `gold_datamart_kpis.py` is visible in its original form.

> **Do not modify these files.** They are a snapshot of the original codebase.
> All active development happens in the dbt project.

## File Mapping

| PySpark Source | dbt Model(s) |
|---------------|--------------|
| `pipeline/silver_customer_layer.py` | `dbt_project/models/silver/customer_silver.sql` |
| `pipeline/gold_datamart_kpis.py` (lines 152-202) | `dbt_project/models/gold/basket_analysis_per_store.sql` |
| `pipeline/gold_datamart_kpis.py` (lines 206-283) | `dbt_project/models/gold/product_trend_per_store.sql` |
| `pipeline/gold_datamart_kpis.py` (lines 287-322) | `dbt_project/models/gold/nb_clients_per_store.sql` |
| `pipeline/airflow_dag_silver_gold.py` | `airflow/dags/vusion_dbt_pipeline.py` |

## Known Issues in the PySpark Code

### 1. Join Bug (`gold_datamart_kpis.py:265`)

The store join in `product_trend_per_store` uses the wrong column:

```python
# ORIGINAL (buggy): joins stores on product_id instead of store_id
.join(stores_df.select("id", "type"),
      F.col("store_id") == F.col("product_id"),  # line 265
      "left")
```

Fixed in the dbt model (`product_trend_per_store.sql`):

```sql
-- FIXED: join stores on store_id
left join stores s on wt.store_id = s.store_id
```

### 2. Duplicate Column (`gold_datamart_kpis.py:166,177`)

`avg_basket_item_count` (line 166) and `avg_unique_items_per_basket` (line 177)
compute the identical expression `F.avg("basket_item_count")`. The duplicate
was removed in the dbt model.

### 3. RFM Segmentation Differences

The dbt model intentionally diverges from the PySpark RFM segment definitions
to better match standard RFM analysis conventions:

| Segment | PySpark (line 209) | dbt |
|---------|-------------------|-----|
| "At Risk" | `recency >= 4 AND frequency <= 2` | `recency <= 2 AND frequency >= 3 AND monetary >= 3` |
| "Hibernating" | `recency <= 2 AND frequency >= 3` | `recency <= 2 AND frequency >= 2` |

The PySpark "At Risk" definition (high recency + low frequency) describes
customers who buy rarely but recently, which is closer to "Need Attention" in
standard RFM. The dbt version uses the conventional definition: formerly
valuable customers whose recency has dropped.

### 4. Monetary Value Definition

The PySpark code uses `sum(quantity)` as the monetary proxy (line 165:
`"Monetary: Total quantity purchased (assuming quantity as proxy for value)"`).
The dbt model uses `sum(spend)` to align with the standard RFM definition
where Monetary represents total spending.

## Contents

```
reference/
├── README.md              # This file
├── INSTRUCTIONS.md        # Original test brief (verbatim from DataEngineeringTest)
└── pipeline/
    ├── silver_customer_layer.py    # Silver layer: RFM scoring, segmentation (370 lines)
    ├── gold_datamart_kpis.py       # Gold layer: basket, trend, client KPIs (382 lines)
    ├── airflow_dag_silver_gold.py  # Original Airflow DAG with spark-submit (305 lines)
    └── spark_conf/
        └── log4j2.xml             # Log4j config for local Spark execution
```
