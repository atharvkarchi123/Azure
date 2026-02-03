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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
import os

# COMMAND ----------

races_schema = StructType(fields=[StructField("race_Id", IntegerType(), False),
                                  StructField("race_year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuit_Id", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.csv(os.getenv('raw_container')+'/incremental/'+partition+'/races.csv',header=True,schema=races_schema)

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df=races_df.na.replace('\\N','00:00:00',subset=['time'])

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit,current_time

races_with_timestamp_df = races_df.withColumn("source", lit(source)) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                  .withColumn("ingestion_date", current_timestamp())\
                                  .withColumn('partition',lit(partition))


# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

write_table(races_with_timestamp_df,'races','l1','src.race_id = tgt.race_id AND src.partition = tgt.partition')

# COMMAND ----------

display(spark.table('uc_f1_db.l1.races'))
print(spark.table('uc_f1_db.l1.races').count())

# COMMAND ----------

dbutils.notebook.exit("\nAll races processed")