# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")   # crea un parámetro que se le pasa a la notebook
v_data_source = dbutils.widgets.get("p_data_source") # obtiene el valor del parámetro pasado y lo almacena en una variable

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "Formula1/2.includes/configuration"

# COMMAND ----------

# MAGIC %run "Formula1/2.includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType  # para cambiar el schema(datatypes) manualmente, mas performante

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),    # una vez tenemos el schema construido, lo aplicamos al df en la próxima cell
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
                                     ]
                             )

# COMMAND ----------

circuits_df = spark.read\
                        .option("header", True)\
                        .schema(circuits_schema)\
                        .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv") #.option("inferSchema", True) ajusta el schema solo. Es para small data (menos performante)

# COMMAND ----------

#display(circuits_df)

# COMMAND ----------

#circuits_df.printSchema()

# COMMAND ----------

#circuits_df.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

#display(circuits_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns as require

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
                                          .withColumnRenamed("circuitRef", "circuit_ref") \
                                          .withColumnRenamed("lat", "latitude") \
                                          .withColumnRenamed("lng", "longitude") \
                                          .withColumnRenamed("alt", "altitude")\
                                          .withColumn("data_source", lit(v_data_source))\
                                          .withColumn("file_date", lit(v_file_date))
 

# COMMAND ----------

#display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df) 

# COMMAND ----------

#display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write data to datalake as parquet

# COMMAND ----------

# circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")  # en el curso se almacena en ambos lugares en una sola linea, debido a los mount
# circuits_final_df.write.mode("overwrite").saveAsTable("f1_processed.circuits")          # y las tablas externas
### Delta Lake:
circuits_final_df.write.format("delta").mode("overwrite").save(f"{processed_folder_path}/circuits")
circuits_final_df.write.format("delta").mode("overwrite").saveAsTable("f1_processed.circuits")

# COMMAND ----------

##display(spark.read.parquet("abfss://processed@formula1dl1610.dfs.core.windows.net/circuits"))
display(spark.read.format("delta").load("abfss://processed@formula1dl1610.dfs.core.windows.net/circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.circuits
# MAGIC --SELECT file_date, COUNT(*) FROM f1_processed.circuits GROUP BY file_date

# COMMAND ----------

dbutils.notebook.exit("Success")