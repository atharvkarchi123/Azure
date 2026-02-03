# Databricks notebook source
dbutils.widgets.text("Source", "Databricks Notebook", "Source of Ingestion")

# COMMAND ----------

source=dbutils.widgets.get("Source")

# COMMAND ----------

import os
from pyspark.sql.functions import current_timestamp,lit

# COMMAND ----------

custructors_schema='constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING'

# COMMAND ----------

constructors_df=spark.read.schema(custructors_schema).csv(os.getenv('raw_container') +'/full_load' +'/constructors.csv',header=True).drop('url') \
    .withColumn('source', lit(source)) \
    .withColumn('ingestion_date', current_timestamp())
display(constructors_df)

# COMMAND ----------

constructors_df=constructors_df.withColumnsRenamed({'constructorId':'constructor_id', 'constructorRef':'constructor_ref'})

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_df.write.mode('overwrite').saveAsTable('uc_f1_db.l1.constructors',path=os.getenv('processed_container')+'/l1/constructors/')

# COMMAND ----------

spark.table('uc_f1_db.l1.constructors').display()

# COMMAND ----------

dbutils.notebook.exit("\nAll constructors processed")