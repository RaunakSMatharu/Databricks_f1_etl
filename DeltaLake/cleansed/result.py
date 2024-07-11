# Databricks notebook source
from datetime import datetime

# COMMAND ----------

def f_get_latest_type2(sourcePath):
    from datetime import datetime
    list_datepart=[]
    for i in dbutils.fs.ls(f'{sourcePath}'):
        list_datepart.append((datetime.strptime(i.name.split('.')[0][-10:].replace('_','-'),"%Y-%m-%d"),i.path))
    list_datepart.sort()
    return(list_datepart[-1][1])

# COMMAND ----------

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

print(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/result'))

# COMMAND ----------

path=f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/result')
df_staging=spark.read.format("delta").load(path)
df_staging.createOrReplaceTempView("result")

# COMMAND ----------

df_staging.columns

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from result

# COMMAND ----------

df_staging.write.mode('overwrite').save('/mnt/silver_sinkstoragegen2acc/result')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.results trg
# MAGIC USING result src
# MAGIC ON trg.resultId = src.resultId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     trg.raceId = src.raceId,
# MAGIC     trg.driverId = src.driverId,
# MAGIC     trg.constructorId = src.constructorId,
# MAGIC     trg.number = src.number,
# MAGIC     trg.grid = src.grid,
# MAGIC     trg.position = src.position,
# MAGIC     trg.positionText = src.positionText,
# MAGIC     trg.positionOrder = src.positionOrder,
# MAGIC     trg.points = src.points,
# MAGIC     trg.laps = src.laps,
# MAGIC     trg.time = src.time,
# MAGIC     trg.milliseconds = src.milliseconds,
# MAGIC     trg.fastestLap = src.fastestLap,
# MAGIC     trg.rank = src.rank,
# MAGIC     trg.fastestLapTime = src.fastestLapTime,
# MAGIC     trg.fastestLapSpeed = src.fastestLapSpeed,
# MAGIC     trg.statusId = src.statusId,
# MAGIC     trg.input_file = src.input_file,
# MAGIC     trg.load_timestamp = src.load_timestamp,
# MAGIC     trg.load_date = src.load_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

