# Databricks notebook source
l=['dim_circuit','dim_driver','dim_race']
for i in l:
    notebookname='DeltaLake/cleansed/'+i
    dbutils.run.notebook(notebookname,0,0)