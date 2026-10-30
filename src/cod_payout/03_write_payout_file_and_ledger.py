# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: write the payout file, then record bags in the ledger
# MAGIC
# MAGIC 1. Writes one Excel workbook for the day: a **Summary** sheet (one row per
# MAGIC    brand), a **Bags** sheet (every bag paid) and an **On hold** sheet
# MAGIC    (bags that failed a check, for finance to review).
# MAGIC 2. Adds every paid bag to `settlement_ledger` with a batch id. That write
# MAGIC    is what marks a bag as settled, so tomorrow's run skips it.
# MAGIC
# MAGIC The ledger is written **after** the file, so a failure here never leaves
# MAGIC bags marked paid without a payout file.

# COMMAND ----------

import os
from datetime import date, datetime, timezone

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "finops_dev")
dbutils.widgets.text("run_date", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date") or date.today().isoformat()
spark.sql(f"USE {catalog}.{schema}")

batch_id = f"PAYOUT-{run_date}-{datetime.now(timezone.utc):%H%M%S}"
out_dir = f"/Volumes/{catalog}/{schema}/payout_files"

# COMMAND ----------

summary = spark.sql("SELECT * FROM brand_payable_summary WHERE bags_paid > 0 ORDER BY brand_id").toPandas()
paid = spark.sql("SELECT * FROM brand_payable_bag WHERE qa_flag = 'OK' ORDER BY brand_id, bag_id").toPandas()
held = spark.sql("SELECT * FROM brand_payable_bag WHERE qa_flag <> 'OK' ORDER BY qa_flag, bag_id").toPandas()

if paid.empty:
    print("Nothing to pay today. No file written, ledger unchanged.")
    dbutils.notebook.exit("nothing_to_pay")

# COMMAND ----------

# Build files on local disk first, then copy them into the volume in one go.
# Excel needs openpyxl. Serverless usually has it; if not, fall back to CSVs
# so the payout file is never skipped.
import shutil
import tempfile

import pandas as pd

stem = f"brand_payout_{run_date}_{batch_id[-6:]}"
tmp = tempfile.mkdtemp()
try:
    import openpyxl  # noqa: F401
    local = [os.path.join(tmp, f"{stem}.xlsx")]
    with pd.ExcelWriter(local[0], engine="openpyxl") as xl:
        summary.to_excel(xl, sheet_name="Summary", index=False)
        paid.to_excel(xl, sheet_name="Bags", index=False)
        held.to_excel(xl, sheet_name="On hold", index=False)
except ImportError:
    local = []
    for name, df in [("summary", summary), ("bags", paid), ("on_hold", held)]:
        p = os.path.join(tmp, f"{stem}_{name}.csv")
        df.to_csv(p, index=False)
        local.append(p)

written = []
for p in local:
    dest = os.path.join(out_dir, os.path.basename(p))
    shutil.copyfile(p, dest)
    written.append(dest)

print(f"Batch {batch_id}: {len(summary)} brands, {len(paid)} bags paid, {len(held)} on hold")
for p in written:
    print("  wrote", p)

# COMMAND ----------

spark.sql("""
  MERGE INTO settlement_ledger AS l
  USING (
    SELECT bag_id, brand_id, net_brand_payable, run_date
    FROM brand_payable_bag
    WHERE qa_flag = 'OK'
  ) AS p
  ON l.bag_id = p.bag_id
  WHEN NOT MATCHED THEN INSERT
    (bag_id, brand_id, net_brand_payable, batch_id, run_date, settled_at)
    VALUES (p.bag_id, p.brand_id, p.net_brand_payable, :batch_id, p.run_date, current_timestamp())
""", args={"batch_id": batch_id})

print("Ledger now holds", spark.table("settlement_ledger").count(), "settled bags")
