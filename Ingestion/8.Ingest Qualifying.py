# Databricks notebook source
dbutils.widgets.text("Source", "Databricks Notebook", "Source of Ingestion")

# COMMAND ----------

# MAGIC %run "/Workspace/Formula 1/Ingestion/utils"

# COMMAND ----------

dbutils.widgets.text('Partition','')

# COMMAND ----------

partition=dbutils.widgets.get("Partition")

# COMMAND ----------

source=dbutils.widgets.get("Source")

# COMMAND ----------

import os 

# COMMAND ----------

qualifying_schema ='qualifyId INT NOT NULL, raceId INT NOT NULL, driverId INT NOT NULL, constructorId INT, number INT, position INT, q1 STRING, q2 STRING, q3 STRING'

# COMMAND ----------

qualifying_df=spark.read.csv(os.getenv('raw_container') +'/incremental/'+partition+ "/quali.csv",schema=qualifying_schema,header=True)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_df=qualifying_df.withColumnsRenamed({"qualifyId":"qualify_id","raceId":"race_id","driverId":"driver_id","constructorId":"constructor_id"}).withColumns({'source' : lit(source),"ingestion_date" : current_timestamp(),'partition':lit(partition)})

# COMMAND ----------

write_table(qualifying_df,'qualifying','l1','src.qualify_id = tgt.qualify_id AND src.partition = tgt.partition')

# COMMAND ----------

display(spark.table('uc_f1_db.l1.qualifying'))
display(spark.table('uc_f1_db.l1.qualifying').count())

# COMMAND ----------

dbutils.notebook.exit("\nAll qualifyings processed")