# Databricks notebook: Mount ADLS Gen2 containers for Formula 1 project
# Widgets:
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("tenant_id", "")
dbutils.widgets.text("client_id", "")
dbutils.widgets.text("client_secret", "")
dbutils.widgets.text("container_bronze", "bronze")
dbutils.widgets.text("container_silver", "silver")
dbutils.widgets.text("container_gold", "gold")

sa = dbutils.widgets.get("storage_account")
tenant = dbutils.widgets.get("tenant_id")
cid = dbutils.widgets.get("client_id")
secret = dbutils.widgets.get("client_secret")
bronze = dbutils.widgets.get("container_bronze")
silver = dbutils.widgets.get("container_silver")
gold   = dbutils.widgets.get("container_gold")

endpoint = f"https://login.microsoftonline.com/{tenant}/oauth2/token"
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": cid,
  "fs.azure.account.oauth2.client.secret": secret,
  "fs.azure.account.oauth2.client.endpoint": endpoint
}

def safe_mount(container, mount_point):
    source = f"abfss://{container}@{sa}.dfs.core.windows.net/"
    try:
        dbutils.fs.mount(source=source, mount_point=mount_point, extra_configs=configs)
        print(f"Mounted {source} -> {mount_point}")
    except Exception as e:
        if "Directory already mounted" in str(e):
            print(f"Already mounted: {mount_point}")
        else:
            raise

safe_mount(bronze, "/mnt/formula1/bronze")
safe_mount(silver, "/mnt/formula1/silver")
safe_mount(gold,   "/mnt/formula1/gold")

display(dbutils.fs.mounts())
