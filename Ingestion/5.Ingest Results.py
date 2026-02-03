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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark.sql.functions import current_timestamp,lit,row_number,asc
from pyspark.sql.window import Window
import os

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read.csv(os.getenv('raw_container')+'/incremental/'+partition+"/results.csv",schema=results_schema,header=True)

# COMMAND ----------

results_df=results_df\
    .withColumnsRenamed({'resultId':'result_id','raceId':'race_id','driverId':'driver_id','constructorId':'constructor_id','positionText':'position_text','positionOrder':'position_order','fastestLap':'fastest_lap','fastestLapTime':'fastest_lap_time','fastestLapSpeed':'fastest_lap_speed'})\
    .withColumns({'source' : lit(source),'ingestion_date': current_timestamp(),'partition' : lit(partition),'row' :row_number().over(Window.partitionBy('race_id','driver_id').orderBy(asc('position_order')))})
results_df=results_df.where('row=1').drop('statusId')

# COMMAND ----------

write_table(results_df,'results','l1','src.result_id=tgt.result_id AND src.partition=tgt.partition')

# COMMAND ----------

display(spark.table('uc_f1_db.l1.results').count())

# COMMAND ----------

dbutils.notebook.exit("\nAll race results processed")