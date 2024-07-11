# Databricks notebook source
# MAGIC %sql
# MAGIC create database if not exists silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists gold

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists bronze

# COMMAND ----------

# %sql
# CREATE external TABLE IF NOT EXISTS silver.laptime (
#   raceId INT,
#   driverId INT,
#   lap INT,
#   position INT,
#   time STRING,
#   milliseconds BIGINT,
#   input_file STRING,
#   load_timestamp TIMESTAMP,
#   load_date DATE
# )
# USING DELTA
# LOCATION '/mnt/silver_sinkstoragegen2acc/laptime/';

# COMMAND ----------

