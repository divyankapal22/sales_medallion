# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "dev")
catalog = dbutils.widgets.get("catalog")

raw_path = f"/Volumes/{catalog}/bronze/raw_data/"
checkpoint_path = f"/Volumes/{catalog}/bronze/checkpoints/sales_autoloader"
target_table = f"{catalog}.bronze.sales_raw"

df_raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(raw_path)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)
dbutils.fs.rm(f"/Volumes/{catalog}/bronze/checkpoints/sales_autoloader", recurse=True)
query = (
    df_raw.writeStream
    .option("checkpointLocation", f"{checkpoint_path}/write_ckpt")
    .trigger(availableNow=True)
    .toTable(target_table)
)
query.awaitTermination()

print(f"✅ Bronze table populated: {target_table}")
display(spark.sql(f"SELECT COUNT(*) AS bronze_row_count FROM {target_table}"))