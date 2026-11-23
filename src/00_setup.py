# Databricks notebook source
dbutils.widgets.text("catalog", "dev")
active_catalog = dbutils.widgets.get("catalog")
print(f"Initializing Unity Catalog objects in: [{active_catalog.upper()}]")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {active_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {active_catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {active_catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {active_catalog}.gold")

# Volume where you will upload sales.csv
spark.sql(f"CREATE VOLUME IF NOT EXISTS {active_catalog}.bronze.raw_data")
# Volume for Auto Loader schema tracking
spark.sql(f"CREATE VOLUME IF NOT EXISTS {active_catalog}.bronze.checkpoints")

# COMMAND ----------

display(spark.sql(f"SHOW VOLUMES IN {active_catalog}.bronze"))