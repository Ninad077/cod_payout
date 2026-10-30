# Databricks notebook source
# MAGIC %md
# MAGIC # Step 4: refresh the bag journey table
# MAGIC
# MAGIC One row per bag showing where it stands in the payout journey. This is
# MAGIC the table the **bag journey dashboard** reads (Looker Studio at Fynd,
# MAGIC via BigQuery; a Databricks dashboard on Free Edition).
# MAGIC
# MAGIC | payout_stage | Meaning |
# MAGIC | --- | --- |
# MAGIC | NOT_COD | Prepaid, no brand payout through this flow |
# MAGIC | NOT_DELIVERED | Still in transit |
# MAGIC | RETURNED | Return started, not paid |
# MAGIC | ON_HOLD | Failed a check today; reason in `hold_reason` |
# MAGIC | SETTLED | In the ledger; `batch_id` says which payout |
# MAGIC | AWAITING_SETTLEMENT | Delivered but not yet picked up by a run |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "finops_dev")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE bag_journey AS
SELECT
  b.bag_id, b.order_id, b.brand_id, b.Brand,
  b.payment_mode, b.Cur_Status, b.Dl_Date,
  CAST(b.Amount_Paid AS DECIMAL(18,2))    AS amount_collected,
  COALESCE(l.net_brand_payable, p.net_brand_payable) AS net_brand_payable,
  CASE
    WHEN UPPER(b.payment_mode) <> 'COD'         THEN 'NOT_COD'
    WHEN l.bag_id IS NOT NULL                   THEN 'SETTLED'
    WHEN p.qa_flag IS NOT NULL AND p.qa_flag <> 'OK' THEN 'ON_HOLD'
    WHEN LOWER(b.Cur_Status) = 'return_initiated' THEN 'RETURNED'
    WHEN b.Dl_Date IS NULL                      THEN 'NOT_DELIVERED'
    ELSE 'AWAITING_SETTLEMENT'
  END                                     AS payout_stage,
  CASE WHEN p.qa_flag <> 'OK' THEN p.qa_flag END AS hold_reason,
  l.batch_id,
  l.settled_at,
  current_timestamp()                     AS refreshed_at
FROM bags b
LEFT JOIN settlement_ledger l ON l.bag_id = b.bag_id
LEFT JOIN brand_payable_bag p ON p.bag_id = b.bag_id
""")

display(spark.sql("""
  SELECT payout_stage, COUNT(*) AS bags, SUM(net_brand_payable) AS payable
  FROM bag_journey GROUP BY payout_stage ORDER BY bags DESC
"""))
