# Databricks notebook source
# MAGIC %md
# MAGIC ##### Read [race_results]

# COMMAND ----------

# MAGIC %run "../2.includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transforming [race_results]

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col

# COMMAND ----------

driver_standings_df = race_results_df\
                                    .groupBy("race_year", "driver_name", "driver_nationality", "team") \
                                    .agg(sum("points").alias("total_points"),
                                        count(when(col("position") == 1, True)).alias("wins"))
    # crea un df con el tablon [race_results] creado previamente, lo agrupa y le aplica agregaciones

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

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")
final_df.write.mode("overwrite").saveAsTable("f1_presentation.drivers_standings")

# COMMAND ----------

# MAGIC %sql 
# MAGIC --SELECT * FROM f1_presentation.drivers_standings