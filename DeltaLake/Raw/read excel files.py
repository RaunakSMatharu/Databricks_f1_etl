# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

def f_add_input_file_name_loadtime_excel_api_sql(df,input_name):
    df_final = df.withColumn('input_file', lit(input_name)) \
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

schema = StructType([
    StructField('constructorId', DoubleType(), True),
    StructField('constructorRef', StringType(), True),
    StructField('name', StringType(), True),
    StructField('nationality', StringType(), True),
    StructField('url', StringType(), True)
])

# COMMAND ----------

df_excel_sheet_1=spark.read.format('com.crealytics.spark.excel').option('header', True).option('sheetName', 'Sheet1').schema(schema).load('/mnt/sourcestorageblobacc/date_part=2024-06-29/constructors_2023_07_29.xlsx')
df_excel_sheet_2=spark.read.format('com.crealytics.spark.excel').option('header', True).option('sheetName', 'Sheet2').schema(schema).load('/mnt/sourcestorageblobacc/date_part=2024-06-29/constructors_2023_07_29.xlsx')
df = df_excel_sheet_2.union(df_excel_sheet_1)

# COMMAND ----------

df=spark.read.table('bronze.constructor')
constructor_schema=df.dtypes

# COMMAND ----------

input_name='constructors_2023_07_29.xlsx'
check_Schema_Validation(df,constructor_schema)
df = f_add_input_file_name_loadtime_excel_api_sql(df, input_name)
#save_dataframe_with_date(df,'/mnt/bronze_sinkstoragegen2acc/constructor')