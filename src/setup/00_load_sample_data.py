# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: load synthetic Fynd-like bags
# MAGIC
# MAGIC Creates the `bags` table the payout job reads. Its columns mirror
# MAGIC `fynd-db.FP_analytics_dwh.avis_base_view`, so the same logic moves to
# MAGIC production by pointing it at the real view.
# MAGIC
# MAGIC Running it again resets everything, including the ledger, so you can
# MAGIC replay the demo from scratch. All data is fake.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "finops_dev")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.sql(f"USE {catalog}.{schema}")
print(f"Loading sample data into {catalog}.{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reset outputs from earlier runs

# COMMAND ----------

for t in [
    "bags_to_settle", "brand_payable_bag", "brand_payable_summary",
    "settlement_ledger", "bag_journey",
]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bags
# MAGIC
# MAGIC 240 bags across 5 brands. Most are delivered COD bags from the last one to three
# MAGIC days. A few are planted on purpose so the checks have something to catch:
# MAGIC three bags whose cash doesn't add up, and one bag with no GST rate.

# COMMAND ----------

import random
from datetime import date, datetime, timedelta, time
from pyspark.sql.types import (StructType, StructField, LongType, StringType,
                               DoubleType, TimestampType)

rng = random.Random(42)
today = date.today()

brands = [  # brand_id, name, company, commission rate
    (101, "Urban Thread", "Urban Thread Pvt Ltd", 0.15),
    (102, "Kora Living", "Kora Homeware LLP", 0.12),
    (103, "Stride Lab", "Stride Footwear Pvt Ltd", 0.18),
    (104, "Neel Ethnic", "Neel Ethnic Wear Pvt Ltd", 0.14),
    (105, "Pixel Kids", "Pixel Kids Retail Pvt Ltd", 0.16),
]
mrps = [499, 799, 999, 1299, 1599, 1999, 2499, 2999, 3499]

def r2(x):
    return round(x + 1e-9, 2)

rows = []
for i in range(240):
    bag_id = 9000001 + i
    brand_id, brand, company, comm_rate = brands[i % len(brands)]

    payment_mode = "COD" if rng.random() < 0.8 else "PREPAID"
    status = rng.choices(["delivery_done", "in_transit", "return_initiated"],
                         weights=[80, 15, 5])[0]
    dl_ts = None
    if status in ("delivery_done", "return_initiated"):
        d = today - timedelta(days=rng.randint(1, 3))  # always before today, whatever the timezone
        dl_ts = datetime.combine(d, time(rng.randint(9, 20), rng.randint(0, 59)))

    mrp = float(rng.choice(mrps))
    store_discount = r2(mrp * rng.choice([0, 0.1, 0.2, 0.3]))
    esp = r2(mrp - store_discount)
    seller_promo = r2(esp * rng.choice([0, 0, 0.05, 0.1]))
    fynd_promo = r2(esp * rng.choice([0, 0, 0, 0.05]))
    coupon = float(rng.choice([0, 0, 0, 50, 100]))
    coupon_category = rng.choice(["fynd", "brand"]) if coupon else None
    fynd_credits = float(rng.choice([0, 0, 0, 0, 25]))
    delivery_charge = float(rng.choice([0, 0, 49]))
    cod_charge = 30.0 if payment_mode == "COD" else 0.0

    brand_funded = seller_promo + (coupon if coupon_category == "brand" else 0)
    fynd_funded = fynd_promo + (coupon if coupon_category == "fynd" else 0) + fynd_credits
    amount_paid = r2(esp - brand_funded - fynd_funded + delivery_charge + cod_charge)

    gst_pct = 5.0 if esp <= 1000 else 12.0
    commission = r2((esp - brand_funded) * comm_rate)
    comm_gst = r2(commission * 0.18)
    payment_charge = r2(amount_paid * 0.02)

    rows.append(dict(
        bag_id=bag_id, order_id=f"FY{bag_id}", shipment_id=f"SH{bag_id}",
        brand_id=brand_id, Brand=brand, Company_Id=brand_id * 10, Company_Name=company,
        Store_Code=f"ST{brand_id}", payment_mode=payment_mode, Cur_Status=status,
        Dl_Date=dl_ts, collectable_by="fynd",
        MRP=mrp, store_discount=store_discount, ESP=esp,
        seller_promotion_discount=seller_promo, fynd_promotion_discount=fynd_promo,
        Coupon_Discount=coupon, coupon_category=coupon_category,
        Fynd_Credits=fynd_credits, Cashback_Used=0.0, loyalty_discount=0.0,
        employee_discount=0.0, delivery_charge=delivery_charge, cod_charge=cod_charge,
        Amount_Paid=amount_paid, commision_charge=commission, comm_cgst_sgst=0.0,
        comm_igst=comm_gst, payment_charge=payment_charge, gst_tax_percentage=gst_pct,
        transfer_price=r2(esp - brand_funded),
    ))

# Planted problems: the first three delivered COD bags collected Rs 100 too much,
# and the fourth has no GST rate. These must be held, not paid.
planted = [r for r in rows if r["payment_mode"] == "COD" and r["Cur_Status"] == "delivery_done"][:4]
for r in planted[:3]:
    r["Amount_Paid"] = r2(r["Amount_Paid"] + 100)
planted[3]["gst_tax_percentage"] = None

money = ["MRP", "store_discount", "ESP", "seller_promotion_discount",
         "fynd_promotion_discount", "Coupon_Discount", "Fynd_Credits", "Cashback_Used",
         "loyalty_discount", "employee_discount", "delivery_charge", "cod_charge",
         "Amount_Paid", "commision_charge", "comm_cgst_sgst", "comm_igst",
         "payment_charge", "gst_tax_percentage", "transfer_price"]
bag_schema = StructType(
    [StructField("bag_id", LongType()), StructField("order_id", StringType()),
     StructField("shipment_id", StringType()), StructField("brand_id", LongType()),
     StructField("Brand", StringType()), StructField("Company_Id", LongType()),
     StructField("Company_Name", StringType()), StructField("Store_Code", StringType()),
     StructField("payment_mode", StringType()), StructField("Cur_Status", StringType()),
     StructField("Dl_Date", TimestampType()), StructField("collectable_by", StringType()),
     StructField("coupon_category", StringType())]
    + [StructField(c, DoubleType()) for c in money]
)
bags = spark.createDataFrame([{f.name: r[f.name] for f in bag_schema} for r in rows], bag_schema)
bags.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("bags")
print(f"bags: {bags.count()} rows")
