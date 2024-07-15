# Databricks notebook source
dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "Formula1/2.includes/common_functions"

# COMMAND ----------

# MAGIC %run "Formula1/2.includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read [race_results]

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
                                                .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_df, 'race_year')

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

#race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
                                                .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transforming [race_results]

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

# driver_standings_df = race_results_df\
#                                     .groupBy("race_year", "driver_name", "driver_nationality", "team") \
#                                     .agg(sum("points").alias("total_points"),
#                                         count(when(col("position") == 1, True)).alias("wins"))
#     # crea un df con el tablon [race_results] creado previamente, lo agrupa y le aplica agregaciones
driver_standings_df = race_results_df \
.groupBy("race_year", "driver_name", "driver_nationality") \
.agg(sum("points").alias("total_points"),
     count(when(col("position") == 1, True)).alias("wins"))

# COMMAND ----------

#display(driver_standings_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

# COMMAND ----------

# Rankear los resultados:
driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))  
# rank specifications: se reinicia el rank por cada año que pasa, el criterio del rank es por desc'total_points' y desempata desc'wins'
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

#display(final_df)
#display(final_df.filter("race_year = 2020"))

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
# final_df.write.mode("overwrite").saveAsTable("f1_presentation.drivers_standings")

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'driver_standings', presentation_folder_path, merge_condition, 'race_year')

# COMMAND ----------

# MAGIC %sql 
# MAGIC --SELECT * FROM f1_presentation.driver_standings
# MAGIC SELECT race_year, COUNT(*) FROM f1_presentation.driver_standings GROUP BY race_year ORDER BY race_year DESC

# COMMAND ----------

display(spark.read.format("delta").load(f"abfss://presentation@formula1dl1610.dfs.core.windows.net/driver_standings"))