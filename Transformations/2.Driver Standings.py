# Databricks notebook source
import os

# COMMAND ----------

# MAGIC %run "/Workspace/Formula 1/Ingestion/utils"

# COMMAND ----------

dbutils.widgets.text('Partition','')

# COMMAND ----------

partition=dbutils.widgets.get('Partition')

# COMMAND ----------

from pyspark.sql.functions import sum, when, count, col, desc, rank, current_timestamp, lit
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Race Results
driver_standings=spark.table('uc_f1_db.l2.race_results').where(col('partition')==partition)

# COMMAND ----------

driver_standings=driver_standings.groupBy('driver_id','constructor_id','year','name','constructor','driver','nationality','nbr')\
    .agg(sum('total_race_points').alias('points'),count(when(col('pos')=='1',True)).alias('wins'))

# COMMAND ----------

driver_standings=driver_standings\
    .withColumns({'pos':rank().over(Window.partitionBy('year').orderBy(desc('points'),desc('wins'))),'created_at' : current_timestamp(),'partition':lit(partition)})

# COMMAND ----------

display(driver_standings) 

# COMMAND ----------

write_table(driver_standings,'driver_standings','l2','src.driver_id=tgt.driver_id AND src.constructor_id=tgt.constructor_id AND src.year=tgt.year AND src.partition=tgt.partition')

# COMMAND ----------

spark.table('uc_f1_db.l2.driver_standings').display()

# COMMAND ----------

spark.table('uc_f1_db.l2.driver_standings').count()