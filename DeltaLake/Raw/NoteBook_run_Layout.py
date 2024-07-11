# Databricks notebook source
l=['DeltaLake/Raw/Read Streaming Files','DeltaLake/Raw/Read_CSV_files','DeltaLake/Raw/Read_SQL_Tables','DeltaLake/Raw/Read_Parquet_files','DeltaLake/Raw/Read_txt_files']

for i in l:
    dbuitsl.run.notebook(i,0,0)
