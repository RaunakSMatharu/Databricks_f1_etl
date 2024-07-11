# Databricks notebook source
from pyspark.sql.functions import col,when,concat,lit
df=spark.read.table('silver.circuits')
df=df.select("circuitId","circuitRef","name","location","country",'alt')
df=df.select([when(col(i)=='\\N',None).otherwise(col(i)).alias(i) for i in df.columns])
df.repartition(1).write.mode("overwrite").save("/mnt/gold_sinkstoragegen2acc/dim_circuits")

# COMMAND ----------

