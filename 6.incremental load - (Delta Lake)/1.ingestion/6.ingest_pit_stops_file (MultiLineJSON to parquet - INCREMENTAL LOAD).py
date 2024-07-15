# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "Formula1/2.includes/configuration"

# COMMAND ----------

# MAGIC %run "Formula1/2.includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read\
                        .schema(pit_stops_schema)\
                        .option("multiLine", True)\
                        .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

#display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

pit_stops_with_ingestion_date_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = pit_stops_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id")\
                                           .withColumnRenamed("raceId", "race_id")\
                                           .withColumn("data_source", lit(v_data_source))\
                                           .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

#display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')
merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")
# final_df.write.mode("overwrite").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

#display(spark.read.parquet("abfss://processed@formula1dl1610.dfs.core.windows.net/pit_stops"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -----SELECT * FROM f1_processed.pit_stops
# MAGIC SELECT COUNT(1), file_date  --# tiene desde las races 1-1047
# MAGIC   FROM f1_processed.pit_stops
# MAGIC GROUP BY file_date

# COMMAND ----------

display(spark.read.format("delta").load(f"abfss://processed@formula1dl1610.dfs.core.windows.net/pit_stops"))

# COMMAND ----------

dbutils.notebook.exit("Success")