# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying json files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../2.includes/configuration"

# COMMAND ----------

# MAGIC %run "../2.includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read\
                        .schema(qualifying_schema)\
                        .option("multiLine", True)\
                        .json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

#qualifying_df.count()

# COMMAND ----------

#display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyingId, driverId, constructorId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = qualifying_with_ingestion_date_df.withColumnRenamed("qualifyId", "qualify_id")\
                                            .withColumnRenamed("driverId", "driver_id")\
                                            .withColumnRenamed("raceId", "race_id")\
                                            .withColumnRenamed("constructorId", "constructor_id")\
                                            .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

#display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")
final_df.write.mode("overwrite").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

#display(spark.read.parquet(f"{processed_folder_path}/qualifying"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM f1_processed.qualifying

# COMMAND ----------

dbutils.notebook.exit("Success")