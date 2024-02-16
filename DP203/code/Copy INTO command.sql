-- Databricks notebook source
CREATE DATABASE appdb

-- COMMAND ----------

USE appdb

-- COMMAND ----------

-- we can create table without specifying cols. Cols will be created from the file copied in the table
create table logdata

-- COMMAND ----------

-- MAGIC %run "../adls-dbfs-setup/3.access_adls_using_service_principal"

-- COMMAND ----------

copy into logdata
from "abfss://parquet@udemystracc.dfs.core.windows.net/Log.parquet"
fileformat = PARQUET
copy_options ('mergeSchema' = 'true')

-- COMMAND ----------

select *
from logdata
