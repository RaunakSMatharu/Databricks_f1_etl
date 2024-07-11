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

print(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/driver'))


# COMMAND ----------


path=f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/driver')
df_staging=spark.read.format("delta").load(path)
df_staging.createOrReplaceTempView("driver")


# COMMAND ----------

df_staging.write.mode('overwrite').save('/mnt/silver_sinkstoragegen2acc/driver')

# COMMAND ----------

df_staging.columns

# COMMAND ----------

# MAGIC %sql
# MAGIC select driverId,count(distinct(driverId)) from driver
# MAGIC group by driverId
# MAGIC having count(distinct(driverId))>1

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC MERGE INTO silver.driver trg
# MAGIC USING driver src
# MAGIC ON trg.driverId = src.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     trg.driverId = src.driverId,
# MAGIC     trg.driverRef = src.driverRef,
# MAGIC     trg.number = src.number,
# MAGIC     trg.code = src.code,
# MAGIC     trg.forename = src.forename,
# MAGIC     trg.surname = src.surname,
# MAGIC     trg.dob = src.dob,
# MAGIC     trg.nationality = src.nationality,
# MAGIC     trg.url = src.url,
# MAGIC     trg.input_file = src.input_file,
# MAGIC     trg.load_timestamp = src.load_timestamp,
# MAGIC     trg.load_date = src.load_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

