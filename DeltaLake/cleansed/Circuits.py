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

print(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/circuit'))

# COMMAND ----------

path=f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/circuit')
df_staging=spark.read.format("delta").load(path)
df_staging.createOrReplaceTempView("circuit")

# COMMAND ----------

df_staging.columns

# COMMAND ----------

help('mack')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.circuits trg
# MAGIC USING circuit src
# MAGIC ON trg.circuitId = src.circuitId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     trg.circuitId = src.circuitId,
# MAGIC     trg.circuitRef=src.circuitRef,
# MAGIC     trg.name=src.name,
# MAGIC     trg.location=src.location,
# MAGIC     trg.country=src.country,
# MAGIC     trg.lat=src.lat,
# MAGIC     trg.lng=src.lng,
# MAGIC     trg.alt=src.alt,
# MAGIC     trg.url=src.url,
# MAGIC     trg.input_file=src.input_file,
# MAGIC     trg.load_timestamp=src.load_timestamp,
# MAGIC     trg.load_date=src.load_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

import mack as mk

# COMMAND ----------

def f_get_primary_key(df):
    try:
        import mack as mk
        primary_key=mk.find_composite_key_candidates(df)
        merge_condition=""
        for i in range(len(primary_key)):
            if(i==(len(primary_key)-1)):
                merge_condition="tgt."+primary_key[i]+"=src."+primary_key[i]
            else:
                merge_condition="tgt."+primary_key[i]+"=src."+primary_key[i]+" AND "
        return merge_condition
    except Exception as err:
        print("error has occured " +err)

print(f_get_primary_key(df_staging))


# COMMAND ----------

print(f_get_primary_key(df_staging))