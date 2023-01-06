# Databricks notebook source
databricks_scope_name = "KeyVaultScope"
storage_account_name = "mcdedevdlake"
storage_account_alias = "dev"

clientID = dbutils.secrets.get(databricks_scope_name, "SPdevdlakeClient")
tenantID = dbutils.secrets.get(databricks_scope_name, "SPdevdlakeTenant")
clientSecret = dbutils.secrets.get(databricks_scope_name, "SPdevdlakeAccess")

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f"{clientID}",
    "fs.azure.account.oauth2.client.secret": f"{clientSecret}",
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenantID}/oauth2/token"
}

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=f"/mnt/{storage_account_alias}/{container_name}",
        extra_configs = configs
    )

# COMMAND ----------

def mount_adls(container_name):
    try:
        dbutils.fs.mount(
            source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
            mount_point=f"/mnt/{storage_account_alias}/{container_name}",
            extra_configs = configs
        )
    except:
        print("Container"+' '+container_name+' '+"already mounted")
