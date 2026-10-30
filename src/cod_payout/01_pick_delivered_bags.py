# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: pick delivered COD bags not yet paid
# MAGIC
# MAGIC A bag qualifies when it is COD, delivered on or before the run date,
# MAGIC Fynd collected the cash, and it is **not already in the settlement
# MAGIC ledger**. That last rule is what makes paying a bag twice impossible,
# MAGIC even if the job is run again the same day.

# COMMAND ----------

from datetime import date

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "finops_dev")
dbutils.widgets.text("run_date", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date") or date.today().isoformat()
spark.sql(f"USE {catalog}.{schema}")
print(f"Run date {run_date} in {catalog}.{schema}")

# COMMAND ----------

# The ledger is the permanent record of every bag ever paid.
spark.sql("""
  CREATE TABLE IF NOT EXISTS settlement_ledger (
    bag_id            BIGINT,
    brand_id          BIGINT,
    net_brand_payable DECIMAL(18,2),
    batch_id          STRING,
    run_date          DATE,
    settled_at        TIMESTAMP
  )
""")

# COMMAND ----------

spark.sql("""
  CREATE OR REPLACE TABLE bags_to_settle AS
  SELECT b.*
  FROM bags AS b
  LEFT ANTI JOIN settlement_ledger AS l
    ON l.bag_id = b.bag_id
  WHERE UPPER(b.payment_mode) = 'COD'
    AND LOWER(b.Cur_Status) = 'delivery_done'   -- CONFIRM exact value(s) at Fynd
    AND b.Dl_Date IS NOT NULL
    AND DATE(b.Dl_Date) <= DATE(:run_date)
    AND LOWER(b.collectable_by) = 'fynd'
""", args={"run_date": run_date})

n = spark.table("bags_to_settle").count()
print(f"{n} bags ready to settle")
