# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import input_file_name

# COMMAND ----------

schema=StructType([
                    StructField('raceId',IntegerType(),True),
                    StructField('driverId',IntegerType(),True),
                    StructField('stop',IntegerType(),True),
                    StructField('lap',IntegerType(),True),
                    StructField('time',TimestampType(),True),
                    StructField('duration',StringType(),True),
                    StructField('milliseconds',IntegerType(),True)
                ])

# COMMAND ----------

from datetime import datetime

def f_write_delta(df, batchId):
    # Get the current date
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # Generate the path with the current date
    path = f'/mnt/bronze_sinkstoragegen2acc/pitstop/date_part={current_date}'
    
    # Write the DataFrame to the generated path in Delta format
    df.write.format('delta').mode('append').save(path)

#def f_write_delta(df,batchId):
    #df.write.format('delta').mode('append').save('/mnt/bronze_sinkstoragegen2acc/pitstop/')

df=spark.readStream.format('csv').option('header', 'true').schema(schema).option('maxfilesperTrigger',1).load('dbfs:/mnt/sourcestoragegen2acc/pitstop/')


df.writeStream.foreachBatch(f_write_delta).outputMode('append').start()

# COMMAND ----------

#df.writeStream.format('console').outputMode('append').start()

# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/sourcestoragegen2acc/pitstop/')

# COMMAND ----------

