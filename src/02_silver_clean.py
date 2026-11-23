# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

dbutils.widgets.text("catalog", "dev")
catalog = dbutils.widgets.get("catalog")

bronze_table = f"{catalog}.bronze.sales_raw"
silver_table = f"{catalog}.silver.sales_cleaned"

df_bronze = spark.table(bronze_table)
initial_count = df_bronze.count()

df_silver = (
    df_bronze
    .filter(F.col("sale_id").isNotNull())
    .filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0))
    .filter(F.col("sale_amount") > 0)
    .withColumn("sale_id", F.col("sale_id").cast(IntegerType()))
    .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
    .withColumn("product_id", F.col("product_id").cast(IntegerType()))
    .withColumn("quantity", F.col("quantity").cast(IntegerType()))
    .withColumn("sale_amount", F.round(F.col("sale_amount").cast(DoubleType()), 2))
    .withColumn("sale_date", F.to_date("sale_date", "yyyy-MM-dd"))
    .withColumn("sale_month", F.date_format("sale_date", "yyyy-MM"))
    .withColumn("region", F.trim(F.col("region")))
    .withColumn("unit_price", F.round(F.col("sale_amount") / F.col("quantity"), 2))
    .dropDuplicates(["sale_id"])
    .select(
        "sale_id", "sale_date", "sale_month", "customer_id",
        "product_id", "quantity", "unit_price", "sale_amount", "region"
    )
)

df_silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table)

clean_count = spark.table(silver_table).count()
print(f"✅ Silver table populated: {silver_table} | Kept {clean_count} of {initial_count} rows (Dropped {initial_count - clean_count} invalid rows)")