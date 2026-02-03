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
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

pitstops_schema = 'raceId INT NOT NULL, driverID INT, stop STRING, lap INT, time STRING, duration DOUBLE, milliseconds INT'

# COMMAND ----------

pitstops_df = spark.read.schema(pitstops_schema).csv(os.getenv('raw_container')+'/incremental/'+partition+'/pit_stops.csv',header=True)

# COMMAND ----------

pitstops_df=pitstops_df.withColumnsRenamed({'raceId':'race_id','driverId':'driver_id'})\
    .withColumns({'source' : lit(source),'ingestion_date':current_timestamp(),'partition':lit(partition)})

# COMMAND ----------

write_table(pitstops_df,'pit_stops','l1','src.race_id=tgt.race_id AND src.driver_id=tgt.driver_id AND src.stop=tgt.stop AND src.partition=tgt.partition')

# COMMAND ----------

display(spark.table('uc_f1_db.l1.pit_stops'))

# COMMAND ----------

spark.table('uc_f1_db.l1.pit_stops').count()

# COMMAND ----------

dbutils.notebook.exit("\nAll pitstops processed")