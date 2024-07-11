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


path=f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/constructor')
df_staging=spark.read.format("delta").load(path)
df_staging.createOrReplaceTempView("constructor")


# COMMAND ----------

# MAGIC %sql
# MAGIC WITH deduplicated_src AS (
# MAGIC   SELECT
# MAGIC     src.*,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY constructorId ORDER BY load_timestamp DESC) as row_num
# MAGIC   FROM
# MAGIC     constructor src
# MAGIC )
# MAGIC MERGE INTO silver.constructor trg
# MAGIC USING (
# MAGIC   SELECT * FROM deduplicated_src WHERE row_num = 1
# MAGIC ) src
# MAGIC ON trg.constructorId = src.constructorId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     trg.constructorId = src.constructorId,
# MAGIC     trg.constructorRef = src.constructorRef,
# MAGIC     trg.name = src.name,
# MAGIC     trg.nationality = src.nationality,
# MAGIC     trg.url = src.url,
# MAGIC     trg.input_file = src.input_file,
# MAGIC     trg.load_timestamp = src.load_timestamp,
# MAGIC     trg.load_date = src.load_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     constructorId, constructorRef, name, nationality, url, input_file, load_timestamp, load_date
# MAGIC   ) VALUES (
# MAGIC     src.constructorId, src.constructorRef, src.name, src.nationality, src.url, src.input_file, src.load_timestamp, src.load_date
# MAGIC   )

# COMMAND ----------

