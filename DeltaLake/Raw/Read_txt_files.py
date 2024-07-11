# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime
schema=StructType([StructField('raceId',IntegerType(),True),
                   StructField('driverId',IntegerType(),True),
                   StructField('lap',IntegerType(),True),
                   StructField('position',IntegerType(),True),
                   StructField('time',StringType(),True),
                   StructField('milliseconds',LongType(),True)
                   ])

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

df=spark.read.table('bronze.laptime')
laptime_schema=df.dtypes[0:-3]

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.read.format('csv').schema(schema).option('header', True).load('/mnt/sourcestoragegen2acc/laptime')
check_Schema_Validation(df,laptime_schema)
df = f_add_input_file_name_loadtime(df)
save_dataframe_with_date(df, '/mnt/bronze_sinkstoragegen2acc/laptime')

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.fs.ls('/mnt/sourcestoragegen2acc/laptime')

# COMMAND ----------

df

# COMMAND ----------

