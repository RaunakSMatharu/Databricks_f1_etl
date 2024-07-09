# Databricks notebook source
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

# COMMAND ----------

from delta.tables import DeltaTable
#dbutils.widgets.text("database","")
#tablename=dbutils.widgets.get("database")

l=['silver','gold']
for database in l:
    spark.sql("USE {0}".format(database))
    df=spark.sql('show tables').collect()
    list_table=[i.tableName for i in df]
    #print(list_table)
    for tablename in list_table:
        full_table_name = f"{database}.{tablename}"
        deltatable=DeltaTable.forName(spark,full_table_name)
        deltatable.vacuum(3)

# COMMAND ----------

# MAGIC %sql
# MAGIC use silver;
# MAGIC show tables;

# COMMAND ----------



deltatable=DeltaTable.forName(spark,'silver.race')
deltatable.vaccum(3)
