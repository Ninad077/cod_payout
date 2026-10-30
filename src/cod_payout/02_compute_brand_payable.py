# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2: work out each brand's payout
# MAGIC
# MAGIC One rule: a discount the brand pays for lowers the brand's money; a
# MAGIC discount Fynd pays for does not. Fynd then keeps its commission (with
# MAGIC GST), its payment fee, and the taxes it must hold back.
# MAGIC
# MAGIC Every bag is also checked: its numbers must add up to the cash the
# MAGIC customer paid. Bags that fail are flagged and **held**, not paid.
# MAGIC
# MAGIC Lines marked CONFIRM are business rules to verify with finance/tax.

# COMMAND ----------

from datetime import date

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "finops_dev")
dbutils.widgets.text("run_date", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_date = dbutils.widgets.get("run_date") or date.today().isoformat()
spark.sql(f"USE {catalog}.{schema}")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE brand_payable_bag AS
WITH
cfg AS (
  SELECT
    CAST(0.005 AS DECIMAL(6,4)) AS gst_tcs_rate,   -- GST TCS u/s 52. CONFIRM current rate
    CAST(0.001 AS DECIMAL(6,4)) AS tds_194o_rate,  -- TDS u/s 194-O.  CONFIRM current rate
    CAST(0.50  AS DECIMAL(6,2)) AS tolerance       -- rupee tolerance for the cash check
),

-- Split every rupee by who funds it
components AS (
  SELECT
    e.bag_id, e.order_id, e.shipment_id, e.Dl_Date,
    e.brand_id, e.Brand, e.Company_Id, e.Company_Name, e.Store_Code,
    CAST(COALESCE(e.MRP, 0) AS DECIMAL(18,4))                                  AS mrp,
    CAST(COALESCE(e.store_discount, 0) AS DECIMAL(18,4))                       AS store_discount,
    CAST(COALESCE(e.ESP, e.MRP - COALESCE(e.store_discount, 0)) AS DECIMAL(18,4)) AS selling_price,

    -- Brand-funded: reduces what the brand earns
    CAST(COALESCE(e.seller_promotion_discount, 0)
      + CASE WHEN LOWER(e.coupon_category) IN ('seller', 'brand')            -- CONFIRM category values
             THEN COALESCE(e.Coupon_Discount, 0) ELSE 0 END AS DECIMAL(18,4)) AS brand_funded_discount,

    -- Fynd-funded: Fynd absorbs it, the brand is paid as if it never happened
    CAST(COALESCE(e.fynd_promotion_discount, 0)
      + CASE WHEN LOWER(e.coupon_category) IN ('seller', 'brand')
             THEN 0 ELSE COALESCE(e.Coupon_Discount, 0) END
      + COALESCE(e.Fynd_Credits, 0)
      + COALESCE(e.Cashback_Used, 0)
      + COALESCE(e.loyalty_discount, 0)                                      -- CONFIRM bearer
      + COALESCE(e.employee_discount, 0) AS DECIMAL(18,4))                   -- CONFIRM bearer
                                                                             AS fynd_funded_discount,

    CAST(COALESCE(e.delivery_charge, 0) AS DECIMAL(18,4))                     AS delivery_charge,
    CAST(COALESCE(e.cod_charge, 0) AS DECIMAL(18,4))                          AS cod_charge,
    CAST(COALESCE(e.commision_charge, 0) AS DECIMAL(18,4))                    AS commission,
    CAST(COALESCE(e.comm_cgst_sgst, 0) + COALESCE(e.comm_igst, 0) AS DECIMAL(18,4)) AS gst_on_commission,
    CAST(COALESCE(e.payment_charge, 0) AS DECIMAL(18,4))                      AS payment_charge,
    CAST(COALESCE(e.gst_tax_percentage, 0) AS DECIMAL(9,4))                   AS gst_pct,
    CAST(COALESCE(e.Amount_Paid, 0) AS DECIMAL(18,4))                         AS amount_collected,
    CAST(e.transfer_price AS DECIMAL(18,4))                                   AS transfer_price
  FROM bags_to_settle e
),

waterfall AS (
  SELECT
    c.*,
    c.selling_price - c.brand_funded_discount                              AS brand_gross_value,
    (c.selling_price - c.brand_funded_discount) / (1 + c.gst_pct / 100)    AS brand_taxable_value,
    c.selling_price - c.brand_funded_discount - c.fynd_funded_discount
      + c.delivery_charge + c.cod_charge - c.amount_collected              AS cash_diff
  FROM components c
)

SELECT
  w.bag_id, w.order_id, w.shipment_id, w.Dl_Date,
  w.brand_id, w.Brand, w.Company_Id, w.Company_Name, w.Store_Code,
  CAST(w.mrp                   AS DECIMAL(18,2)) AS mrp,
  CAST(w.store_discount        AS DECIMAL(18,2)) AS store_discount,
  CAST(w.selling_price         AS DECIMAL(18,2)) AS selling_price,
  CAST(w.brand_funded_discount AS DECIMAL(18,2)) AS brand_funded_discount,
  CAST(w.fynd_funded_discount  AS DECIMAL(18,2)) AS fynd_funded_discount,
  CAST(w.brand_gross_value     AS DECIMAL(18,2)) AS brand_gross_value,
  CAST(w.brand_taxable_value   AS DECIMAL(18,2)) AS brand_taxable_value,
  CAST(w.commission            AS DECIMAL(18,2)) AS commission,
  CAST(w.gst_on_commission     AS DECIMAL(18,2)) AS gst_on_commission,
  CAST(w.payment_charge        AS DECIMAL(18,2)) AS payment_charge,
  CAST(w.brand_taxable_value * cfg.gst_tcs_rate  AS DECIMAL(18,2)) AS gst_tcs,
  CAST(w.brand_taxable_value * cfg.tds_194o_rate AS DECIMAL(18,2)) AS tds_194o,   -- CONFIRM base
  CAST(
      w.brand_gross_value - w.commission - w.gst_on_commission - w.payment_charge
    - w.brand_taxable_value * cfg.gst_tcs_rate
    - w.brand_taxable_value * cfg.tds_194o_rate
  AS DECIMAL(18,2))                              AS net_brand_payable,
  CAST(w.amount_collected AS DECIMAL(18,2))      AS amount_collected,
  CAST(w.cash_diff        AS DECIMAL(18,2))      AS chk_customer_math_diff,
  CAST(w.brand_gross_value - w.transfer_price AS DECIMAL(18,2)) AS chk_transfer_price_diff,
  CASE
    WHEN w.gst_pct = 0                              THEN 'MISSING_GST_RATE'
    WHEN w.brand_gross_value - w.commission <= 0    THEN 'NON_POSITIVE_PAYABLE'
    WHEN ABS(w.cash_diff) > cfg.tolerance           THEN 'CUSTOMER_MATH_MISMATCH'
    ELSE 'OK'
  END                                            AS qa_flag,
  DATE(:run_date)                                AS run_date
FROM waterfall w
CROSS JOIN cfg
""", args={"run_date": run_date})

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE brand_payable_summary AS
SELECT
  run_date, brand_id, Brand, Company_Id, Company_Name,
  COUNT_IF(qa_flag = 'OK')                                              AS bags_paid,
  COUNT_IF(qa_flag <> 'OK')                                             AS bags_on_hold,
  SUM(CASE WHEN qa_flag = 'OK' THEN brand_gross_value END)              AS gross_value,
  SUM(CASE WHEN qa_flag = 'OK' THEN commission + gst_on_commission END) AS commission_incl_gst,
  SUM(CASE WHEN qa_flag = 'OK' THEN payment_charge END)                 AS payment_charges,
  SUM(CASE WHEN qa_flag = 'OK' THEN gst_tcs END)                        AS gst_tcs,
  SUM(CASE WHEN qa_flag = 'OK' THEN tds_194o END)                       AS tds_194o,
  SUM(CASE WHEN qa_flag = 'OK' THEN net_brand_payable END)              AS net_payable,
  SUM(CASE WHEN qa_flag <> 'OK' THEN net_brand_payable END)             AS amount_on_hold
FROM brand_payable_bag
GROUP BY run_date, brand_id, Brand, Company_Id, Company_Name
""")

display(spark.sql("SELECT qa_flag, COUNT(*) AS bags FROM brand_payable_bag GROUP BY qa_flag"))
display(spark.table("brand_payable_summary").orderBy("brand_id"))
