# Databricks notebook source
import os

# COMMAND ----------

raw_container=os.getenv('raw_container')
processed_container=os.getenv('processed_container')
presentation_container=os.getenv('presentation_container')

# COMMAND ----------

dbutils.notebook.run('/Workspace/Formula 1/Ingestion/Orchestration/1.Create Databases',0,{'raw' : raw_container+'/l0/','processed':processed_container+'/l1/','presentation':presentation_container+'/l2/'})

# COMMAND ----------

dbutils.notebook.run('/Workspace/Formula 1/Ingestion/Orchestration/2.Ingest Raw Data',0,{'circuits':raw_container+'/full_load/circuits.csv','constructors':raw_container+'/full_load/constructors.csv','drivers':raw_container+'/full_load/drivers.csv','sprint_results' : raw_container+'/full_load/sprint_results.csv'})