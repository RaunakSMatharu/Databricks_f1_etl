# Databricks notebook source
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

import requests
headers = { 
           'x-rapidapi-host': f_get_secret('api-link'),#"v1.formula-1.api-sports.io",
           'x-rapidapi-key': f_get_secret('api-key')
}

response=requests.get("https://v1.formula-1.api-sports.io/teams",headers=headers)

data=response.json()['response']


#get the column names
for response in data:
    column_name=list(response.keys())
    break

#print(column_name)
data_list=[]

for response in data:
    data_list.append((response['id'],response['name'],response['logo'],response['base'],response['first_team_entry'],response['world_championships'],response['highest_race_finish'],response['pole_positions'],response['fastest_laps'],response['president'],response['director'],response['technical_manager'],response['chassis'],response['engine'],response['tyres']))

df=spark.createDataFrame(data_list,schema=column_name)


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

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

df_bronze=spark.read.table('bronze.teams')
teams_schema=df_bronze.dtypes[0:-3]

# COMMAND ----------

input_file='Teams'
check_Schema_Validation(df,teams_schema)
df = f_add_input_file_name_loadtime_excel_api_sql(df,input_file)
df=df.withColumn("input_file", concat(df.input_file, lit("_"), date_format(current_date(), "yyyy_MM_dd"),lit("_API")))
#df.write.mode('overwrite').save('/mnt/bronze_sinkstoragegen2acc/teams/')
save_dataframe_with_date(df,'/mnt/bronze_sinkstoragegen2acc/teams')

# COMMAND ----------

