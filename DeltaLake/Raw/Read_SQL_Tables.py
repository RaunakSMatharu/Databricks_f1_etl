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

jdbc=f_get_secret('source-sql-jdbc')
user=f_get_secret('source-sql-user')
password=f_get_secret('source-sql-password')

# COMMAND ----------

qualify_df = (
    spark.read.format("jdbc")
    .option("url", jdbc)
    .option("dbtable", "dbo.qualifying")
    .option("user", user)
    .option("password", password)
    .load()
)
#qualify_df.write.mode('overwrite').save('/mnt/bronze_sinkstoragegen2acc/qualifying/')

# COMMAND ----------

df_bronze=spark.read.table('bronze.qualifying')
qualifying_schema=df_bronze.dtypes[0:-3]

# COMMAND ----------

input_file='Qualify'
check_Schema_Validation(qualify_df,qualifying_schema)
df = f_add_input_file_name_loadtime_excel_api_sql(qualify_df,input_file)

df=df.withColumn("input_file", concat(df.input_file, lit("_"), date_format(current_date(), "yyyy_MM_dd"),lit("_SQL")))
save_dataframe_with_date(df,'/mnt/bronze_sinkstoragegen2acc/qualifying')


# COMMAND ----------

