# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

def f_add_input_file_name_loadtime(df):
    df_final = df.withColumn('input_file', col('_metadata.file_name')) \
                     .withColumn('load_timestamp', current_timestamp()) \
                         .withColumn('load_date',to_date(col('load_timestamp')))                      
    return df_final

def check_Schema_Validation(df,sink_schema):
    source_schema=df.limit(1).dtypes
    if source_schema==sink_schema:
        return 0
    else:
        raise Exception('Schema Mismatch')


def save_dataframe_with_date(df: DataFrame, base_path: str) -> None:
    current_date = datetime.now().strftime('%Y-%m-%d')
    full_path = f'{base_path}/date_part={current_date}'
    df.write.mode('overwrite').save(full_path)
    
    print(f'DataFrame saved to: {full_path}')

# COMMAND ----------

# result_schema=[('resultId', 'bigint'),
#  ('raceId', 'bigint'),
#  ('driverId', 'bigint'),
#  ('constructorId', 'bigint'),
#  ('number', 'bigint'),
#  ('grid', 'bigint'),
#  ('position', 'bigint'),
#  ('positionText', 'string'),
#  ('positionOrder', 'bigint'),
#  ('points', 'double'),
#  ('laps', 'bigint'),
#  ('time', 'string'),
#  ('milliseconds', 'bigint'),
#  ('fastestLap', 'bigint'),
#  ('rank', 'bigint'),
#  ('fastestLapTime', 'string'),
#  ('fastestLapSpeed', 'double'),
#  ('statusId', 'bigint')]
df=spark.read.table('bronze.result')
result_schema=df.dtypes[0:-3]

# COMMAND ----------

df = spark.read.format('parquet').option('inferSchema', True).option('header', True).load('dbfs:/mnt/sourcestoragegen2acc/result/')
check_Schema_Validation(df,result_schema)
df = f_add_input_file_name_loadtime(df)
#df.write.mode('overwrite').save('/mnt/bronze_sinkstoragegen2acc/result/')
save_dataframe_with_date(df,'/mnt/bronze_sinkstoragegen2acc/result')