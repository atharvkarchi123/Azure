# Databricks notebook source
dbutils.widgets.text("Source", "Databricks Notebook", "Source of Ingestion")

# COMMAND ----------

source=dbutils.widgets.get("Source")

# COMMAND ----------

import os
from pyspark.sql.functions import col, concat, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

# COMMAND ----------

drivers_df=spark.read.csv(os.getenv('raw_container')+'/full_load'+'/drivers.csv', schema=drivers_schema,header=True)
display(drivers_df)

# COMMAND ----------

drivers_df=drivers_df.withColumnsRenamed({'driverId':'driver_id','driverRef':'driver_ref'}).drop('url').withColumns({'source':lit(source),'ingestion_date':current_timestamp()})
display(drivers_df)

# COMMAND ----------

drivers_df.write.mode('overwrite').option('path',os.getenv('processed_container')+'/l1/drivers/').saveAsTable('uc_f1_db.l1.drivers')
display(spark.table('uc_f1_db.l1.drivers'))

# COMMAND ----------

dbutils.notebook.exit("\nAll drivers processed")