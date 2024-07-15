# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_df):   # agrega columna 'ingestion_date' al dataframe pasado por parametro
  output_df = input_df.withColumn("ingestion_date", current_timestamp())
  return output_df

# COMMAND ----------

def re_arrange_partition_column(input_df, partition_column):    # coloca la partition_column al final, asi lo requiere spark para hacer la incremental load 2
  column_list = []
  for column_name in input_df.schema.names:
    if column_name != partition_column:
      column_list.append(column_name)
  column_list.append(partition_column)
  output_df = input_df.select(column_list)
  return output_df

# COMMAND ----------

def overwrite_partition(input_df, db_name, table_name, partition_column):
  output_df = re_arrange_partition_column(input_df, partition_column)   #obtiene el df ordenado ya que asi lo requiere. Solo funciona con las external tables. Por esto. no altera los datos del contenedor
  spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")  # a partir de este momento los contenedores quedan inutilizados ya que no cargan data con esta funcion
  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
    output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
  else:
    output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
  df_row_list = input_df.select(column_name) \
                        .distinct() \
                        .collect()
  
  column_value_list = [row[column_name] for row in df_row_list]
  return column_value_list

# COMMAND ----------

def merge_delta_data(input_df, db_name, table_name, folder_path, merge_condition, partition_column): 
  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")   # para optimizar las consultas en tablas particionadas
  from delta.tables import DeltaTable

  if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):   # tabla existe?
    deltaTable = DeltaTable.forPath(spark, f"{folder_path}/{table_name}")       # true: merge en la partition
    deltaTable.alias("tgt").merge(
                                  input_df.alias("src"),
                                  merge_condition)\
                            .whenMatchedUpdateAll()\
                            .whenNotMatchedInsertAll()\
                            .execute()
    input_df.createOrReplaceTempView("temp_src_table")      # YA QUE NO HAY MOUNTS se hizo primero en el contenedor y luego ahora en las tablas del hive_metastore 
    spark.sql(f"""
              MERGE INTO {db_name}.{table_name} tgt
              USING temp_src_table src
              ON {merge_condition}
              WHEN MATCHED THEN UPDATE SET *
              WHEN NOT MATCHED THEN INSERT *
              """)
  else:                                                                         # false: crea e inserta data en la partition
    input_df.write.mode("overwrite").partitionBy(partition_column).format("delta").saveAsTable(f"{db_name}.{table_name}")
    input_df.write.format("delta").mode("overwrite").save(f"{folder_path}/{table_name}")# esto lo agregué yo ya que no hay mounts. para seguir con el curso guardando tablas delta en el contenedor. 
                                                                                        # Si bien crea la tabla en el contenedor, no puede actualizarla