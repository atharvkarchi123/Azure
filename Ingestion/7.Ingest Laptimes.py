# Databricks notebook source
dbutils.widgets.text("Source", "Databricks Notebook", "Source of Ingestion")

# COMMAND ----------

dbutils.widgets.text('Partition','')

# COMMAND ----------

# MAGIC %run "/Workspace/Formula 1/Ingestion/utils"

# COMMAND ----------

partition=dbutils.widgets.get("Partition")

# COMMAND ----------

source=dbutils.widgets.get("Source")

# COMMAND ----------

import os 
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

laptimes_schema = 'raceId INT NOT NULL, driverID INT NOT NULL, lap INT NOT NULL, position INT, time STRING, milliseconds INT'

# COMMAND ----------

laptimes_df=spark.read.csv(os.getenv('raw_container') +'/incremental/'+partition+ '/lap_times.csv',schema=laptimes_schema,header=True)

# COMMAND ----------

laptimes_df=laptimes_df.withColumnsRenamed({'raceId':'race_id','driverID':'driver_id'})\
    .withColumns({'soruce' : lit(source),'ingestion_date' : current_timestamp(),'partition' : lit(partition)})

# COMMAND ----------

write_table(laptimes_df,'lap_times','l1','src.race_id=tgt.race_id AND src.driver_id=tgt.driver_id AND src.lap=tgt.lap AND src.partition=tgt.partition')

# COMMAND ----------

display(spark.table('uc_f1_db.l1.lap_times'))

# COMMAND ----------

spark.table('uc_f1_db.l1.lap_times').count()

# COMMAND ----------

dbutils.notebook.exit("\nAll laptimes processed")