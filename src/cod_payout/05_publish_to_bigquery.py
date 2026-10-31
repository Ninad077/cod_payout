# Databricks notebook source
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "finops_dev")
dbutils.widgets.text("bq_dataset", "")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
BQ_DATASET = dbutils.widgets.get("bq_dataset")

if not BQ_DATASET:
    print("No BigQuery dataset set for this target. Skipping publish.")
    dbutils.notebook.exit("skipped")


for table in ["bag_journey", "brand_payable_summary"]:
    (spark.table(f"{catalog}.{schema}.{table}")
        .write.format("bigquery")
        .option("writeMethod", "direct")
        .mode("overwrite")
        .save(f"{BQ_DATASET}.{table}"))
    print(f"Published {table} to {BQ_DATASET}.{table}")