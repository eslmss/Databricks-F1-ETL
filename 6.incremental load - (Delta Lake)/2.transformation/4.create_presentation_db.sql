-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_presentation
--LOCATION "abfss://presentation@formula1dl1610.dfs.core.windows.net"
-- en el curso se crearon External tables especificando la LOCATION en el mount presentation. Con la student sub no es posible crear mounts, así que se las creara en la ubicación default (hive)

-- COMMAND ----------

DESC DATABASE f1_processed;