# Databricks notebook: Initialize configuration & utility functions
# Usage: Run this once per cluster to set paths in Spark conf.
# Pass optional widgets: storage_account, container_bronze/silver/gold, use_mounts
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, lit
import json, yaml

dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("container_bronze", "bronze")
dbutils.widgets.text("container_silver", "silver")
dbutils.widgets.text("container_gold", "gold")
dbutils.widgets.dropdown("use_mounts", "false", ["false","true"])

storage_account = dbutils.widgets.get("storage_account")
container_bronze = dbutils.widgets.get("container_bronze")
container_silver = dbutils.widgets.get("container_silver")
container_gold   = dbutils.widgets.get("container_gold")
use_mounts       = dbutils.widgets.get("use_mounts") == "true"

if use_mounts:
    bronze_base = f"/mnt/formula1/bronze"
    silver_base = f"/mnt/formula1/silver"
    gold_base   = f"/mnt/formula1/gold"
else:
    if not storage_account:
        raise ValueError("storage_account widget must be set when use_mounts=false")
    bronze_base = f"abfss://{container_bronze}@{storage_account}.dfs.core.windows.net"
    silver_base = f"abfss://{container_silver}@{storage_account}.dfs.core.windows.net"
    gold_base   = f"abfss://{container_gold}@{storage_account}.dfs.core.windows.net"

spark.conf.set("f1.paths.bronze", bronze_base)
spark.conf.set("f1.paths.silver", silver_base)
spark.conf.set("f1.paths.gold",   gold_base)

print("Configured paths:")
print("BRONZE:", bronze_base)
print("SILVER:", silver_base)
print("GOLD:  ", gold_base)

def delta_path(layer: str, name: str) -> str:
    base = spark.conf.get(f"f1.paths.{layer}")
    return f"{base}/{name}"
