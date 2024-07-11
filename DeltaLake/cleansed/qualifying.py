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


print(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/qualifying'))


# COMMAND ----------


path=f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/qualifying')
df_staging=spark.read.format("delta").load(path)
df_staging.createOrReplaceTempView("qualifying")


# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC MERGE INTO silver.qualifying trg
# MAGIC USING qualifying src
# MAGIC ON trg.qualifyId = src.qualifyId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     trg.qualifyId = src.qualifyId,
# MAGIC     trg.raceId = src.raceId,
# MAGIC     trg.driverId = src.driverId,
# MAGIC     trg.constructorId = src.constructorId,
# MAGIC     trg.number = src.number,
# MAGIC     trg.position = src.position,
# MAGIC     trg.q1 = src.q1,
# MAGIC     trg.q2 = src.q2,
# MAGIC     trg.q3 = src.q3,
# MAGIC     trg.input_file = src.input_file,
# MAGIC     trg.load_timestamp = src.load_timestamp,
# MAGIC     trg.load_date = src.load_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *