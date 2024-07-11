# Databricks notebook source
from datetime import datetime
def f_get_latest_type2(sourcePath):
    from datetime import datetime
    list_datepart=[]
    for i in dbutils.fs.ls(f'{sourcePath}'):
        list_datepart.append((datetime.strptime(i.name.split('.')[0][-10:].replace('_','-'),"%Y-%m-%d"),i.path))
    list_datepart.sort()
    return(list_datepart[-1][1])


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

path=f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/race')
df_staging=spark.read.format("delta").load(path)
df_staging.createOrReplaceTempView("race")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.race trg
# MAGIC USING race src
# MAGIC ON trg.raceId = src.raceId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     trg.raceId = src.raceId,
# MAGIC     trg.year = src.year,
# MAGIC     trg.round = src.round,
# MAGIC     trg.circuitId = src.circuitId,
# MAGIC     trg.name = src.name,
# MAGIC     trg.date = src.date,
# MAGIC     trg.time = src.time,
# MAGIC     trg.url = src.url,
# MAGIC     trg.fp1_date = src.fp1_date,
# MAGIC     trg.fp1_time = src.fp1_time,
# MAGIC     trg.fp2_date = src.fp2_date,
# MAGIC     trg.fp2_time = src.fp2_time,
# MAGIC     trg.fp3_date = src.fp3_date,
# MAGIC     trg.fp3_time = src.fp3_time,
# MAGIC     trg.quali_date = src.quali_date,
# MAGIC     trg.quali_time = src.quali_time,
# MAGIC     trg.sprint_date = src.sprint_date,
# MAGIC     trg.sprint_time = src.sprint_time,
# MAGIC     trg.input_file = src.input_file,
# MAGIC     trg.load_timestamp = src.load_timestamp,
# MAGIC     trg.load_date = src.load_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

