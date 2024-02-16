# Databricks notebook source
# MAGIC %run "../adls-dbfs-setup/3.access_adls_using_service_principal"

# COMMAND ----------

json_df = spark.read.format("json").load("abfss://json@udemystracc.dfs.core.windows.net/customer.json")
display(json_df)

# COMMAND ----------

from pyspark.sql.functions import *
newjson = json_df.select(col("customerid"),col("registered"),explode(col("courses")).alias("courses"))
display(newjson)

# COMMAND ----------

json_df2 = spark.read.format("json").load("abfss://json@udemystracc.dfs.core.windows.net/customer_updated.json")
filtered_df = json_df2.filter(json_df2['courses'].isNotNull())

newdf = filtered_df.select(col("customerid"),col("registered"),explode(col("courses")).alias("courses"), col("details.city"), col("details.mobile"))

display(newdf)
