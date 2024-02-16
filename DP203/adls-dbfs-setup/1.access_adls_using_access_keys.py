# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Azure Data Lake using Access keys (gives full access to the storage account for the user/notebook)
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List the files in the desired container
# MAGIC 3. Read data from the container

# COMMAND ----------

spark.conf.set("fs.azure.account.key.udemystracc.dfs.core.windows.net",
               "U6l/jpFNToRfezjfXNRwgUrFSKMFzMbQqqODLms2elWHO+yVm+sdQspU9wxkD6aJhpgNyn/jUo5u+AStTszvKQ==")

# COMMAND ----------

display(dbutils.fs.ls("abfss://parquet@udemystracc.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.parquet("abfss://parquet@udemystracc.dfs.core.windows.net/Log.parquet"))
