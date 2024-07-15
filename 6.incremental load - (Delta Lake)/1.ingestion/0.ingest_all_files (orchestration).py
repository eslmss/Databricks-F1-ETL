# Databricks notebook source
# MAGIC %md
# MAGIC ### Databricks Workflow Orchestration: 
# MAGIC ##### no es lo ideal hacerla en Databricks, es mejor Azure Data Factory ya que facilita por ejemplo, la ejecucion de varias notebooks simultáneamente. Para hacerlo en Databricks, revisar S14, Notebook Workflows

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuits_file (csv to parquet)", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file (csv to parquet)", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file (SingleLineJSON to parquet)", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file (SingleLineNestedJSON to parquet)", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file (SingleLineJSON to parquet)", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stops_file (MultiLineJSON to parquet)", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_file (folderOfSplittedCSVs to parquet)", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_file (folderOfSplittedMultiLineJSONs to parquet)", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

v_result