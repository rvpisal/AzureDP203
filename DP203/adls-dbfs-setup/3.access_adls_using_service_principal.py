# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 2. Generate a secret/password for the application
# MAGIC 3. Set Spark config with App/Client ID, Directory/ Tenant ID & secret
# MAGIC 4. Assign Role 'Storgae Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = "9a7827a5-bb6c-484e-b185-bbfe90406acf"
tenant_id = "e5bc154a-c8ec-4634-bddb-b6d7d2bb2663"
client_secret = "jBO8Q~PhYdsi45HRBi~MEcy43OkpcYQMdQEitb1a"

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
