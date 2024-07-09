# Databricks notebook source
dbutils.widgets.text("notebook_path","")
notebook_path=dbutils.widget.get('notebook_path')
dbutils.widgets.text("tablename","")
tablename=dbutils.widget.get('tablename')

# COMMAND ----------

if(tablename == ""):
    dbutils.notebook.run(f'{notebook_path}', 0)
else:
    dbutils.notebook.run(f'{notebook_path}', 0, {"tablename": f"{tablename}"})
