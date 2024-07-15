# Databricks notebook source
dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "Formula1/2.includes/common_functions"

# COMMAND ----------

# MAGIC %run "Formula1/2.includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the required data for the Joins

# COMMAND ----------

# drivers_df = spark.read.parquet(f"{processed_folder_path}/drivers")\
#                                 .withColumnRenamed("number", "driver_number")\
#                                 .withColumnRenamed("name", "driver_name")\
#                                 .withColumnRenamed("nationality", "driver_nationality") # renombrando columnas para que no haya conflictos ni confusión con las demás tablas
drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
.withColumnRenamed("number", "driver_number") \
.withColumnRenamed("name", "driver_name") \
.withColumnRenamed("nationality", "driver_nationality") 

# COMMAND ----------

# constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors") \
#                                     .withColumnRenamed("name", "team")
constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
.withColumnRenamed("name", "team") 

# COMMAND ----------

# circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")\
#                                 .withColumnRenamed("location", "circuit_location") 
circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
.withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

# races_df = spark.read.parquet(f"{processed_folder_path}/races")\
#                                 .withColumnRenamed("name", "race_name")\
#                                 .withColumnRenamed("race_timestamp", "race_date") 
races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
.withColumnRenamed("name", "race_name") \
.withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

# ## con esta tabla hay que tener cuidado y usar Filter porque se le aplica Incremental Load. Con todas las anteriores no porque son Full Load
# results_df = spark.read.parquet(f"{processed_folder_path}/results") \
#                                 .filter(f"file_date = '{v_file_date}'") \
#                                 .withColumnRenamed("time", "race_time")\
#                                 .withColumnRenamed("race_id", "result_race_id")
results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
.filter(f"file_date = '{v_file_date}'") \
.withColumnRenamed("time", "race_time") \
.withColumnRenamed("race_id", "result_race_id") \
.withColumnRenamed("file_date", "result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Se unen los df de afuera hacia adentro. La tabla [results] tiene link con 3 de las tablas requeridas ([races], [drivers], [constructors]). 
# MAGIC #### Entonces primero, la tabla [circuits] se la INNER JOINea a [races] y luego [results] INNER JOIN a lo obtenido, siguiendo con [drivers], [constructors]
# MAGIC #### Esto permite 2 transfomaciones en lugar de 5

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join circuits to races

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
                                .select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join results to all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.result_race_id == race_circuits_df.race_id)\
                                .join(drivers_df, results_df.driver_id == drivers_df.driver_id)\
                                    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)
                # si no se especifica es INNER JOIN

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Final df with selected columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position", "result_file_date") \
                          .withColumn("created_date", current_timestamp()) \
                          .withColumnRenamed("result_file_date", "file_date") 

# COMMAND ----------

#display(final_df)

# COMMAND ----------

#display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc())) # para chequear si corresponde con la pag

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")
# final_df.write.mode("overwrite").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id') # solo guarda la tabla en el hive_metastore
merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT race_id, COUNT(*) FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC

# COMMAND ----------

display(spark.read.format("delta").load(f"abfss://presentation@formula1dl1610.dfs.core.windows.net/race_results"))