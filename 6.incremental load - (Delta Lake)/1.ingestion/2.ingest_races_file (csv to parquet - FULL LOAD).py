# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

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
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType  # para cambiar el schema(datatypes) manualmente, mas performante

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)
                                  ]
                          )

# COMMAND ----------

races_df = spark.read\
                    .option("header", True)\
                    .schema(races_schema)\
                    .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

#display(races_df)

# COMMAND ----------

#races_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, col, lit

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                                  .withColumn("data_source", lit(v_data_source))\
                                  .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_with_ingestion_date_df = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the required columns & renaming as required

# COMMAND ----------

races_selected_df = races_with_ingestion_date_df.select(col('raceId').alias('race_id'), 
                                                        col('year').alias('race_year'), 
                                                        col('round'), 
                                                        col('circuitId').alias('circuit_id'),
                                                        col('name'), 
                                                        col('ingestion_date'),
                                                        col('data_source'),
                                                        col('race_timestamp'),
                                                        col('file_date'))

# COMMAND ----------

#display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write the output to processed container in parquet format. PartitionBy('race_year')

# COMMAND ----------

# races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")
# races_selected_df.write.mode("overwrite").saveAsTable("f1_processed.races")
### Delta Lake:
races_selected_df.write.format("delta").mode("overwrite").save(f"{processed_folder_path}/races")
races_selected_df.write.format("delta").mode("overwrite").saveAsTable("f1_processed.races")

# COMMAND ----------

## display(spark.read.parquet("abfss://processed@formula1dl1610.dfs.core.windows.net/races"))
display(spark.read.format("delta").load("abfss://processed@formula1dl1610.dfs.core.windows.net/races"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races

# COMMAND ----------

dbutils.notebook.exit("Success")