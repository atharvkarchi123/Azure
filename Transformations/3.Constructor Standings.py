# Databricks notebook source
import os

# COMMAND ----------

# MAGIC %run "/Workspace/Formula 1/Ingestion/utils"

# COMMAND ----------

dbutils.widgets.text('Partition','')

# COMMAND ----------

partition=dbutils.widgets.get('Partition')

# COMMAND ----------

from pyspark.sql.functions import sum,current_timestamp,desc,count,rank,when,col,lit
from pyspark.sql.window import Window

# COMMAND ----------

constructor_standings=spark.table('uc_f1_db.l2.race_results').where(col('partition')==partition)

# COMMAND ----------

constructor_standings=constructor_standings.groupBy('constructor_id','year','constructor','constructor_nationality')\
    .agg(sum('points').alias('points'),count(when(col('pos')=='1',True)).alias('wins'))\
    .withColumns({'pos' : rank().over(Window.partitionBy('year').orderBy(desc('points'),desc('wins')))\
        , 'created_at' : current_timestamp(),'partition':lit(partition)})\
    .withColumnRenamed('constructor_nationality' , 'nationality')

# COMMAND ----------

display(constructor_standings)

# COMMAND ----------

write_table(constructor_standings,'constructor_standings','l2','src.constructor_id=tgt.constructor_id AND src.year=tgt.year AND src.partition=tgt.partition')

# COMMAND ----------

spark.table('uc_f1_db.l2.constructor_standings').display()