# Databricks notebook source

dbutils.widgets.text("input_path", "")
dbutils.widgets.text("tablename", "")  # Define the "tablename" input widget
tablename = dbutils.widgets.get("tablename")
print(tablename)

# COMMAND ----------

df = spark.sql(f"SELECT * FROM silver.{tablename}")

# COMMAND ----------

def f_get_primary_key(df):
    try:
        import mack as mk
        primary_key=mk.find_composite_key_candidates(df)
        merge_condition=""
        for i in range(len(primary_key)):
            if(i==(len(primary_key)-1)):
                merge_condition="tgt."+primary_key[i]+"=src."+primary_key[i]
            else:
                merge_condition="tgt."+primary_key[i]+"=src."+primary_key[i]+" AND "
        return merge_condition
    except Exception as err:
        print("error has occured " +err)



# COMMAND ----------

print(f_get_primary_key(df))