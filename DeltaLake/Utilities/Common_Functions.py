# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# Function to fetch secret
#mountpoint for source
def f_get_secret(key):
    return dbutils.secrets.get(scope="scoperaunak", key=key)

# Configuration dictionary with OAuth credentials
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f_get_secret(key="devclient-id"),
    "fs.azure.account.oauth2.client.secret": f_get_secret(key="devclient-secret"),
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{f_get_secret(key='tenant-id')}/oauth2/token" # Ensure this is the full URL with protocol
}

# COMMAND ----------

def f_add_input_file_name_loadtime(df):
    df_final = df.withColumn('input_file', col('_metadata.file_name')) \
                     .withColumn('load_timestamp', current_timestamp()) \
                         .withColumn('load_date',to_date(col('load_timestamp')))                      
    return df_final

# COMMAND ----------

def check_Schema_Validation(df,sink_schema):
    source_schema=df.limit(1).dtypes
    if source_schema==sink_schema:
        return 0
    else:
        raise Exception('Schema Mismatch')

# COMMAND ----------

def save_dataframe_with_date(df: DataFrame, base_path: str) -> None:
    current_date = datetime.now().strftime('%Y-%m-%d')
    full_path = f'{base_path}/date_part_{current_date}'
    df.write.mode('overwrite').save(full_path)
    
    print(f'DataFrame saved to: {full_path}')

# COMMAND ----------

def save_dataframe_with_date(df: DataFrame, base_path: str) -> None:
    current_date = datetime.now().strftime('%Y-%m-%d')
    full_path = f'{base_path}/date_part_{current_date}'
    df.write.mode('overwrite').save(full_path)
    
    print(f'DataFrame saved to: {full_path}')

# COMMAND ----------

dbutils.fs.ls('/mnt/sourcestoragegen2acc/laptime/')

# COMMAND ----------

for i in dbutils.fs.ls('/mnt/sourcestoragegen2acc/laptime/'):
    print(i.name.split('.')[0][-10:])


# COMMAND ----------

def f_get_latest_type2(sourcePath):
    from datetime import datetime
    list_datepart=[]
    for i in dbutils.fs.ls(f'{sourcePath}'):
        list_datepart.append((datetime.strptime(i.name.split('.')[0][-10:].replace('_','-'),"%Y-%m-%d"),i.path))
    list_datepart.sort()
    return(list_datepart[-1][1])

# COMMAND ----------

