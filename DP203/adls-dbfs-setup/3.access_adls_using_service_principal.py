# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set Spark config with App/Client ID, Directory/ Tenant ID & secret
# MAGIC 4. Assign Role 'Storgae Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = "kv-UdemyDP203", key = "sp-client-id")
tenant_id = dbutils.secrets.get(scope = "kv-UdemyDP203", key = "sp-tenant-id")
client_secret = dbutils.secrets.get(scope = "kv-UdemyDP203", key = "sp-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.udemystracc.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.udemystracc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.udemystracc.dfs.core.windows.net",client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.udemystracc.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.udemystracc.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# display(dbutils.fs.ls("abfss://json@udemystracc.dfs.core.windows.net"))

# COMMAND ----------

# display(spark.read.parquet("abfss://parquet@udemystracc.dfs.core.windows.net/Log.parquet"))
