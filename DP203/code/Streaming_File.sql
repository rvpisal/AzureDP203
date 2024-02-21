-- Databricks notebook source
-- MAGIC %run "../adls-dbfs-setup/3.access_adls_using_service_principal"

-- COMMAND ----------

create table DimCustomer(
  CustomerID string
  ,CompanyName string
  ,SalesPerson string
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('abfss://csv@udemystracc.dfs.core.windows.net/Customer')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC file_path = 'abfss://csv@udemystracc.dfs.core.windows.net/Customer/'
-- MAGIC checkpoint_location = 'abfss://csv@udemystracc.dfs.core.windows.net/customer_checkpoint'
-- MAGIC schema_location = 'abfss://csv@udemystracc.dfs.core.windows.net/customer_schema'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.readStream.format('cloudfiles').option('cloudFiles.schemaLocation',schema_location)\
-- MAGIC .option('cloudFiles.format','csv').load(file_path)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # checkpoint_location to ensure when new file is added, where to start from
-- MAGIC df.writeStream.format('delta')\
-- MAGIC     .option ("checkpointLocation",checkpoint_location)\
-- MAGIC     .option("mergeSchema",True)\
-- MAGIC     .table('DimCustomer')

-- COMMAND ----------

select *
from dimcustomer
