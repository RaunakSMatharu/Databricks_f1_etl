# Databricks notebook source
from datetime import datetime
def f_get_latest_type3(sourcePath: str):
    try:
        list_file_path = []
        for i in dbutils.fs.ls(f'{sourcePath}'):
            if i.name != '_delta_log/':
                list_file_path.append((i.path, i.name.split('=')[1].replace('/', '')))
                
        list_file_path.sort(key=lambda x: datetime.strptime(x[1], "%Y-%m-%d"),reverse=True)
        return (list_file_path[0][0])
    except Exception as err:
        print('error occurred', str(err))

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/circuit'))
spark.sql("DROP TABLE IF EXISTS bronze.circuit")
df.write.format('delta').saveAsTable('bronze.circuit')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/race'))
spark.sql("DROP TABLE IF EXISTS bronze.race")
df.write.format('delta').saveAsTable('bronze.race')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/driver'))
spark.sql("DROP TABLE IF EXISTS bronze.driver")
df.write.format('delta').saveAsTable('bronze.driver')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/qualifying'))
spark.sql("DROP TABLE IF EXISTS bronze.qualifying")
df.write.format('delta').saveAsTable('bronze.qualifying')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/laptime'))
spark.sql("DROP TABLE IF EXISTS bronze.laptime")
df.write.format('delta').saveAsTable('bronze.laptime')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/pitstop'))
spark.sql("DROP TABLE IF EXISTS bronze.pitstop")
df.write.format('delta').saveAsTable('bronze.pitstop')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/qualifying'))
spark.sql("DROP TABLE IF EXISTS bronze.qualifying")
df.write.format('delta').saveAsTable('bronze.qualifying')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/result'))
spark.sql("DROP TABLE IF EXISTS bronze.result")
df.write.format('delta').saveAsTable('bronze.result')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/teams'))
spark.sql("DROP TABLE IF EXISTS bronze.teams")
df.write.format('delta').saveAsTable('bronze.teams')