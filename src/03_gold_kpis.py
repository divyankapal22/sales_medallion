# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "dev")
catalog = dbutils.widgets.get("catalog")

df_silver = spark.table(f"{catalog}.silver.sales_cleaned")

# KPI 1: Regional Sales Performance
df_regional = (
    df_silver.groupBy("region")
    .agg(
        F.round(F.sum("sale_amount"), 2).alias("total_revenue"),
        F.countDistinct("sale_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_sold"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.round(F.avg("sale_amount"), 2).alias("avg_order_value")
    )
    .orderBy(F.desc("total_revenue"))
)
df_regional.write.mode("overwrite").saveAsTable(f"{catalog}.gold.kpi_regional_sales")

# KPI 2: Monthly Revenue Trend
df_monthly = (
    df_silver.groupBy("sale_month")
    .agg(
        F.round(F.sum("sale_amount"), 2).alias("monthly_revenue"),
        F.countDistinct("sale_id").alias("total_orders"),
        F.sum("quantity").alias("units_sold")
    )
    .orderBy("sale_month")
)
df_monthly.write.mode("overwrite").saveAsTable(f"{catalog}.gold.kpi_monthly_revenue")

# KPI 3: Top-Performing Products
df_products = (
    df_silver.groupBy("product_id")
    .agg(
        F.round(F.sum("sale_amount"), 2).alias("product_revenue"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("sale_id").alias("times_ordered"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price")
    )
    .orderBy(F.desc("product_revenue"))
)
df_products.write.mode("overwrite").saveAsTable(f"{catalog}.gold.kpi_top_products")

# KPI 4: Customer Lifetime Value (CLV) & Tiering
df_clv = (
    df_silver.groupBy("customer_id")
    .agg(
        F.round(F.sum("sale_amount"), 2).alias("lifetime_spend"),
        F.countDistinct("sale_id").alias("total_orders"),
        F.sum("quantity").alias("total_items_bought"),
        F.min("sale_date").alias("first_purchase_date"),
        F.max("sale_date").alias("last_purchase_date")
    )
    .withColumn(
        "customer_tier",
        F.when(F.col("lifetime_spend") >= 50000, "VIP")
         .when(F.col("lifetime_spend") >= 25000, "Loyal")
         .otherwise("Standard")
    )
    .orderBy(F.desc("lifetime_spend"))
)
df_clv.write.mode("overwrite").saveAsTable(f"{catalog}.gold.kpi_customer_ltv")

print(f"✅ All 4 Gold KPI tables populated in {catalog}.gold")
display(spark.sql(f"SHOW TABLES IN {catalog}.gold"))