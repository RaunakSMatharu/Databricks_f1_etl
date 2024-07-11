# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_driver=spark.read.table('silver.driver')

# COMMAND ----------

from pyspark.sql.functions import concat, lit, when, col

df_driver = df_driver.withColumn(
    'full_name',
    concat(df_driver.forename, lit(' '), df_driver.surname)
).withColumn(
    "number",
    when(df_driver.number == "\\N", None).otherwise(col("number"))
).drop(
    "url", "input_file", "load_timestamp", "load_date", "forename", "surname"
)

df_driver.repartition(1).write.mode("overwrite").save("/mnt/gold_sinkstoragegen2acc/dim_driver")

# COMMAND ----------

# MAGIC %md
# MAGIC