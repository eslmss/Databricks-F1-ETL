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

constructors_standing_df = race_results_df\
                                    .groupBy("race_year", "team")\
                                    .agg(sum("points").alias("total_points"),
                                        count(when(col("position") == 1, True)).alias("wins"))
    # crea un df con el tablon [race_results] creado previamente, lo agrupa y le aplica agregaciones

# COMMAND ----------

#display(constructors_standing_df.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc

# COMMAND ----------

# Rankear los resultados:
constructors_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
# rank specifications: se reinicia el rank por cada año que pasa, el criterio del rank es por desc'total_points' y desempata desc'wins'
final_df = constructors_standing_df.withColumn("rank", rank().over(constructors_rank_spec))

# COMMAND ----------

#display(final_df)
#display(final_df.filter("race_year = 2020"))

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructors_standing")
final_df.write.mode("overwrite").saveAsTable("f1_presentation.constructors_standing")

# COMMAND ----------

# MAGIC %sql 
# MAGIC --SELECT * FROM f1_presentation.constructors_standing