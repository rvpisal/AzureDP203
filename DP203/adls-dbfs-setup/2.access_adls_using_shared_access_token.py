# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure Data Lake using SAS token (gives limited time access to the storage account for the user/notebook)
# MAGIC 1. Set the spark config for SAS token
# MAGIC 2. List the files in the desired container
# MAGIC 3. Read data from the container

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.udemystracc.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.udemystracc.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.udemystracc.dfs.core.windows.net", "sp=rl&st=2024-02-07T03:47:08Z&se=2024-02-07T11:47:08Z&spr=https&sv=2022-11-02&sr=c&sig=geltPu4m0YG073xk5ZSP1vJp5Jk1bpW7cW9YmF8x38Y%3D")

# COMMAND ----------

display(dbutils.fs.ls("abfss://parquet@udemystracc.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.parquet("abfss://parquet@udemystracc.dfs.core.windows.net/Log.parquet"))
