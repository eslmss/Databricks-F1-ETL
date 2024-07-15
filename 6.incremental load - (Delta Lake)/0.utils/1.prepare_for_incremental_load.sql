-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Borramos las processed y presentation db para arrancar de nuevo con Incremental Load. 
-- MAGIC ##### Como son managed tables, solo se borraron las tablas pero el contenido de raw, processed y presentation se borra directamente desde el Microsoft Azure Storage Explorer sin tocar el contenedor

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation 