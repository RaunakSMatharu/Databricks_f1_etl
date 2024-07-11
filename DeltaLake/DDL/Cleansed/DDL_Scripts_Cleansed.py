# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists silver

# COMMAND ----------

# MAGIC %md
# MAGIC create table silver.laptime(
# MAGIC   raceId int,
# MAGIC   driverId int,
# MAGIC   lap int,
# MAGIC   position int,
# MAGIC   time string,
# MAGIC   milliseconds long,
# MAGIC   input_file string,
# MAGIC   load_timestamp timestamp,
# MAGIC   load_date date
# MAGIC ) using delta location '/mnt/silver_sinkstoragegen2acc/laptime/'

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/silver_sinkstoragegen2acc/laptime/')
spark.sql("DROP TABLE IF EXISTS silver.laptimes")
df.write.format('delta').saveAsTable('silver.laptimes')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/silver_sinkstoragegen2acc/result/')
spark.sql("DROP TABLE IF EXISTS silver.results")
df.write.format('delta').saveAsTable('silver.results')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/silver_sinkstoragegen2acc/circuit')
spark.sql("DROP TABLE IF EXISTS silver.circuits")
df.write.format('delta').saveAsTable('silver.circuits')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/silver_sinkstoragegen2acc/teams')
spark.sql("DROP TABLE IF EXISTS silver.teams")
df.write.format('delta').saveAsTable('silver.teams')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/silver_sinkstoragegen2acc/constructor')
spark.sql("DROP TABLE IF EXISTS silver.constructor")
df.write.format('delta').saveAsTable('silver.constructor')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/silver_sinkstoragegen2acc/driver')
spark.sql("DROP TABLE IF EXISTS silver.driver")
df.write.format('delta').saveAsTable('silver.driver')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/silver_sinkstoragegen2acc/pitstop')
spark.sql("DROP TABLE IF EXISTS silver.pitstops")
df.write.format('delta').saveAsTable('silver.pitstops')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/silver_sinkstoragegen2acc/qualifying')
spark.sql("DROP TABLE IF EXISTS silver.qualifying")
df.write.format('delta').saveAsTable('silver.qualifying')

# COMMAND ----------

df = spark.read.format("delta").option("header", "true").load('/mnt/silver_sinkstoragegen2acc/race')
spark.sql("DROP TABLE IF EXISTS silver.race")
df.write.format('delta').saveAsTable('silver.race')

# COMMAND ----------

