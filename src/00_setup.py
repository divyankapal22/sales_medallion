# Databricks notebook source
dbutils.widgets.text("catalog", "dev")
active_catalog = dbutils.widgets.get("catalog")
print(f"Initializing Unity Catalog objects in: [{active_catalog.upper()}]")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {active_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {active_catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {active_catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {active_catalog}.gold")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {active_catalog}.bronze.raw_data")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {active_catalog}.bronze.checkpoints")

# COMMAND ----------

# Clean up any leftover DLT Streaming Tables / Materialized Views so notebook writes succeed cleanly
spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {active_catalog}.gold.kpi_regional_sales")
spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {active_catalog}.gold.kpi_monthly_revenue")
spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {active_catalog}.gold.kpi_top_products")
spark.sql(f"DROP MATERIALIZED VIEW IF EXISTS {active_catalog}.gold.kpi_customer_ltv")

spark.sql(f"DROP TABLE IF EXISTS {active_catalog}.silver.sales_cleaned")
spark.sql(f"DROP TABLE IF EXISTS {active_catalog}.bronze.sales_raw")

# Reset Auto Loader checkpoint so sales.csv is re-ingested fresh on each demo run
dbutils.fs.rm(f"/Volumes/{active_catalog}/bronze/checkpoints/sales_autoloader", recurse=True)

print(f"✅ Clean slate ready in [{active_catalog.upper()}]")