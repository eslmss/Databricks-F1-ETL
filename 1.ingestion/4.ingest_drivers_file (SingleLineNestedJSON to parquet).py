# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest drivers.json file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

# inner JSON schema:
name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)
  
])

# COMMAND ----------

# outer JSON schema:
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema), #############
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df = spark.read \
.schema(drivers_schema) \
.json(f"{raw_folder_path}/drivers.json")

# COMMAND ----------

#drivers_df.printSchema()

# COMMAND ----------

#display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. driverId renamed to driver_id  
# MAGIC 1. driverRef renamed to driver_ref  
# MAGIC 1. ingestion date added
# MAGIC 1. name added with concatenation of forename and surname

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id")\
                                                        .withColumnRenamed("driverRef", "driver_ref")\
                                                        .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                                                        .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

#display(drivers_with_columns_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns
# MAGIC 1. name.forename
# MAGIC 1. name.surname
# MAGIC 1. url

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop(col("url"))

# COMMAND ----------

#display(drivers_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")
drivers_final_df.write.mode("overwrite").saveAsTable("f1_processed.drivers")

# COMMAND ----------

#display(spark.read.parquet("abfss://processed@formula1dl1610.dfs.core.windows.net/drivers"))

# COMMAND ----------

# MAGIC %sql
# MAGIC --SELECT * FROM f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit("Success")