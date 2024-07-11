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

print(f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/teams'))


# COMMAND ----------


path=f_get_latest_type3('/mnt/bronze_sinkstoragegen2acc/teams')
df_staging=spark.read.format("delta").load(path)
df_staging.createOrReplaceTempView("team")


# COMMAND ----------

df_staging.write.mode('overwrite').save('/mnt/silver_sinkstoragegen2acc/teams')

# COMMAND ----------

df_staging.columns

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from teams

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver.teams trg
# MAGIC USING silver.teams src -- Specify the correct table alias for the circuit table
# MAGIC ON trg.id = src.id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     trg.id = src.id,
# MAGIC     trg.name = src.name,
# MAGIC     trg.logo = src.logo,
# MAGIC     trg.base = src.base,
# MAGIC     trg.first_team_entry = src.first_team_entry,
# MAGIC     trg.world_championships = src.world_championships,
# MAGIC     trg.highest_race_finish = src.highest_race_finish,
# MAGIC     trg.pole_positions = src.pole_positions,
# MAGIC     trg.fastest_laps = src.fastest_laps,
# MAGIC     trg.president = src.president,
# MAGIC     trg.director = src.director,
# MAGIC     trg.technical_manager = src.technical_manager,
# MAGIC     trg.chassis = src.chassis,
# MAGIC     trg.engine = src.engine,
# MAGIC     trg.tyres = src.tyres,
# MAGIC     trg.input_file = src.input_file,
# MAGIC     trg.load_timestamp = src.load_timestamp,
# MAGIC     trg.load_date = src.load_date
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *

# COMMAND ----------

