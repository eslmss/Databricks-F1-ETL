# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../2.includes/configuration"

# COMMAND ----------

# MAGIC %run "../2.includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"   # esta es otra forma de definir schemas comparada con las anteriores

# COMMAND ----------

constructors_df = spark.read\
                            .schema(constructors_schema)\
                            .json(f"{raw_folder_path}/constructors.json")

# COMMAND ----------

#constructors_df.printSchema()

# COMMAND ----------

#display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop unwanted columns from the dataframe

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(col('url'))

# COMMAND ----------

#display(constructors_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                                .withColumnRenamed("constructorRef", "constructor_ref")\
                                                .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

constructors_final_df = add_ingestion_date(constructors_renamed_df)

# COMMAND ----------

#display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 Write output to parquet file

# COMMAND ----------

constructors_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")
constructors_final_df.write.mode("overwrite").saveAsTable("f1_processed.constructors")

# COMMAND ----------

#display(spark.read.parquet("abfss://processed@formula1dl1610.dfs.core.windows.net/constructors"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM f1_processed.constructors

# COMMAND ----------

dbutils.notebook.exit("Success")