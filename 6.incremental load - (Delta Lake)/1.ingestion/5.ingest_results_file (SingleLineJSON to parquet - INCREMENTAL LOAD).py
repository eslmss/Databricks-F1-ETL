# Databricks notebook source
# MAGIC %md
# MAGIC ### Showing what's inside (2021-03-21, 2021-03-28, 2021-04-18) folders.

# COMMAND ----------

# %sql
# SELECT race_id, driver_id, COUNT(1)
# FROM f1_processed.results
# GROUP BY race_id, driver_id
# HAVING COUNT(1) > 1
# ORDER BY race_id, driver_id DESC

# COMMAND ----------

# spark.read.json("abfss://raw@formula1dl1610.dfs.core.windows.net/2021-03-21/results.json").createOrReplaceTempView("results_cutover")   # tiene desde las races 1-1047
# spark.read.json("abfss://raw@formula1dl1610.dfs.core.windows.net/2021-03-28/results.json").createOrReplaceTempView("results_w1")        # tiene la race 1052
# spark.read.json("abfss://raw@formula1dl1610.dfs.core.windows.net/2021-04-18/results.json").createOrReplaceTempView("results_w2")        # tiene la race 1053

# COMMAND ----------

# %sql
# -- SELECT raceId, COUNT(1)  --# tiene desde las races 1-1047
# --   FROM results_cutover
# -- GROUP BY raceId
# -- ORDER BY raceId DESC

# COMMAND ----------

# %sql
# SELECT raceId, COUNT(1)   --tiene la race 1052
#   FROM results_w1
# GROUP BY raceId
# ORDER BY raceId DESC

# COMMAND ----------

# %sql
# SELECT raceId, COUNT(1)   --tiene la race 1053
#   FROM results_w2
# GROUP BY raceId
# ORDER BY raceId DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest results.json file

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read\
                        .schema(results_schema)\
                        .json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

#results_df.printSchema()

# COMMAND ----------

#display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                                    .withColumn("data_source", lit(v_data_source))\
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

# COMMAND ----------

#display(results_with_ingestion_date_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))

# COMMAND ----------

#display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC De-dupe the dataframe

# COMMAND ----------

results_final_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])    # si encuentra este par de columnas duplicado, deja uno cualquiera

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write to output to processed container in parquet format

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 1er Metodo [no funciona]

# COMMAND ----------

### 1er Metodo para incermental: NO ME FUNCIONO
# for race_id_list in results_final_df.select("race_id").distinct().collect(): #el collect crea una lista, usar solo con small data
#    if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
#      spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### 2do Metodo

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.results

# COMMAND ----------

# spark.sql(f"""
#               MERGE INTO f1_processed.results tgt
#               USING results_final_df src
#               ON tgt.result_id = src.result_id AND tgt.race_id = src.race_id
#               WHEN MATCHED THEN UPDATE SET *
#               WHEN NOT MATCHED THEN INSERT *
#               """)

# COMMAND ----------

##overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id') # solo funciona con las tablas de la db del hive. No modifica el contenido de los contenedores
merge_condition="tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_final_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# results_final_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")
# results_final_df.write.mode("overwrite").saveAsTable("f1_processed.results")

# COMMAND ----------

##display(spark.read.parquet("abfss://processed@formula1dl1610.dfs.core.windows.net/results"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -----SELECT * FROM f1_processed.results -- como vimos al principio, carga 20 registros(carreras) por cada carpeta nueva (la primera trae +1000)
# MAGIC SELECT COUNT(1), file_date  --# tiene desde las races 1-1047
# MAGIC   FROM f1_processed.results
# MAGIC GROUP BY file_date

# COMMAND ----------

# %sql
# SELECT race_id, COUNT(1)  -- tiene desde las races 1-1047
#   FROM f1_processed.results
# GROUP BY race_id
# ORDER BY race_id DESC

# COMMAND ----------

display(spark.read.format("delta").load(f"abfss://processed@formula1dl1610.dfs.core.windows.net/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Boletear: Parche para poder guardar data en contenedor. Siempre sera Full Load, solo se hace para poder continuar con el curso

# COMMAND ----------

# from pyspark.sql import SparkSession
# sparkSession = SparkSession.builder.appName("query").getOrCreate()

# COMMAND ----------

# query_df = spark.sql("""SELECT * FROM f1_processed.results""")  

# COMMAND ----------

# display(query_df)

# COMMAND ----------

# query_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")
# query_df.write.format("delta").mode("overwrite").save(f"{processed_folder_path}/results")

# COMMAND ----------

# display(spark.read.parquet(f"{processed_folder_path}/results"))