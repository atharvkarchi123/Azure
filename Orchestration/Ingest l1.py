# Databricks notebook source
dbutils.widgets.text("Source", "Databricks Notebook", "Source of Ingestion")

# COMMAND ----------

s='Results:'

# COMMAND ----------

s+=dbutils.notebook.run("/Workspace/Formula 1/Ingestion/1.Ingest Circuits", 0, {"Source": dbutils.widgets.get("Source")})

# COMMAND ----------

s+=dbutils.notebook.run("/Workspace/Formula 1/Ingestion/2.Ingest Races", 0, {"Source": dbutils.widgets.get("Source")})

# COMMAND ----------

s+=dbutils.notebook.run("/Workspace/Formula 1/Ingestion/3.Ingest Constructors", 0, {"Source": dbutils.widgets.get("Source")})

# COMMAND ----------

s+=dbutils.notebook.run("/Workspace/Formula 1/Ingestion/4.Ingest Drivers", 0, {"Source": dbutils.widgets.get("Source")})

# COMMAND ----------

s+=dbutils.notebook.run("/Workspace/Formula 1/Ingestion/5.Ingest Results", 0, {"Source": dbutils.widgets.get("Source")})

# COMMAND ----------

s+=dbutils.notebook.run("/Workspace/Formula 1/Ingestion/6.Ingest pitstops", 0, {"Source": dbutils.widgets.get("Source")})

# COMMAND ----------

s+=dbutils.notebook.run("/Workspace/Formula 1/Ingestion/7.Ingest Laptimes", 0, {"Source": dbutils.widgets.get("Source")})

# COMMAND ----------

s+=dbutils.notebook.run("/Workspace/Formula 1/Ingestion/8.Ingest Qualifying", 0, {"Source": dbutils.widgets.get("Source")})

# COMMAND ----------

dbutils.notebook.exit(s+'\nAll files successfully ingested')