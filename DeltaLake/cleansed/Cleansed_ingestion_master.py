# Databricks notebook source
l=['Circuits','constructor','driver','Laptime','pitstop','qualifying','race','result','teams']
for i in l:
    notebookname='DeltaLake/cleansed/'+i
    dbutils.run.notebook(notebookname,0,0)

# COMMAND ----------

