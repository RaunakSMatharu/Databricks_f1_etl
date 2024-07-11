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

df=spark.read.table('bronze.circuit')
circuit_schema=df.dtypes[0:-3]

# COMMAND ----------

# circuit_schema=[('circuitId', 'int'),
#  ('circuitRef', 'string'),
#  ('name', 'string'),
#  ('location', 'string'),
#  ('country', 'string'),
#  ('lat', 'double'),
#  ('lng', 'double'),
#  ('alt', 'string'),
#  ('url', 'string')]


# COMMAND ----------

# Load the DataFrame
df = spark.read.format('csv').option('inferSchema', True).option('header', True).load('dbfs:/mnt/sourcestoragegen2acc/circuit').cache()
check_Schema_Validation(df,circuit_schema)
df = f_add_input_file_name_loadtime(df)
save_dataframe_with_date(df,'/mnt/bronze_sinkstoragegen2acc/circuit')

# COMMAND ----------

# race_schema=[('raceId', 'int'),
#  ('year', 'int'),
#  ('round', 'int'),
#  ('circuitId', 'int'),
#  ('name', 'string'),
#  ('date', 'date'),
#  ('time', 'string'),
#  ('url', 'string'),
#  ('fp1_date', 'string'),
#  ('fp1_time', 'string'),
#  ('fp2_date', 'string'),
#  ('fp2_time', 'string'),
#  ('fp3_date', 'string'),
#  ('fp3_time', 'string'),
#  ('quali_date', 'string'),
#  ('quali_time', 'string'),
#  ('sprint_date', 'string'),
#  ('sprint_time', 'string')]

# COMMAND ----------

df=spark.read.table('bronze.constructor')
constructor_schema=df.dtypes[0:-3]

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).load('dbfs:/mnt/sourcestoragegen2acc/race').cache()
check_Schema_Validation(df,race_schema)
df = f_add_input_file_name_loadtime(df)
save_dataframe_with_date(df,'/mnt/bronze_sinkstoragegen2acc/race')
df.unpersist()

# COMMAND ----------

# driver_schema=[('driverId', 'int'),
#  ('driverRef', 'string'),
#  ('number', 'string'),
#  ('code', 'string'),
#  ('forename', 'string'),
#  ('surname', 'string'),
#  ('dob', 'date'),
#  ('nationality', 'string'),
#  ('url', 'string')]

# COMMAND ----------

df=spark.read.table('bronze.driver')
driver_schema=df.dtypes[0:-3]

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).load('dbfs:/mnt/sourcestoragegen2acc/driver').cache()
check_Schema_Validation(df,driver_schema)
df = f_add_input_file_name_loadtime(df)
save_dataframe_with_date(df,'/mnt/bronze_sinkstoragegen2acc/driver')

# COMMAND ----------

