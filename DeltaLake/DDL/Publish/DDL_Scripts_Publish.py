# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists gold

# COMMAND ----------

dbutils.fs.ls('/mnt/gold_sinkstoragegen2acc/fact_results/')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/gold_sinkstoragegen2acc/fact_results/')
spark.sql("DROP TABLE IF EXISTS gold.fact")
df.write.format('delta').saveAsTable('gold.fact')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/gold_sinkstoragegen2acc/dim_driver/')
spark.sql("DROP TABLE IF EXISTS gold.dim_driver")
df.write.format('delta').saveAsTable('gold.dim_driver')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/gold_sinkstoragegen2acc/dim_circuits/')
spark.sql("DROP TABLE IF EXISTS gold.dim_circuits")
df.write.format('delta').saveAsTable('gold.dim_circuits')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/gold_sinkstoragegen2acc/dim_race/')
spark.sql("DROP TABLE IF EXISTS gold.dim_race")
df.write.format('delta').saveAsTable('gold.dim_race')

# COMMAND ----------

