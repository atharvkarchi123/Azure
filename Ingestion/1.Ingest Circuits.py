# Databricks notebook source
dbutils.widgets.text("Source", "Databricks Notebook", "Source of Ingestion")

# COMMAND ----------

source=dbutils.widgets.get("Source")

# COMMAND ----------

import os

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuit_Id", IntegerType(), False),
                                     StructField("circuit_Ref", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("latitude", DoubleType(), True),
                                     StructField("longitude", DoubleType(), True),
                                     StructField("altitude", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(os.getenv('raw_container') +'/full_load'+'/circuits.csv/')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuit_Id"), col("circuit_Ref"), col("name"), col("location"), col("country"), col("latitude"), col("longitude"), col("altitude"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_final_df = circuits_selected_df.withColumn('soruce', lit(source)) \
                                        .withColumn("ingestion_date" , current_timestamp()) 

# COMMAND ----------

circuits_final_df.write.mode("overwrite").saveAsTable('uc_f1_db.l1.circuits',path=os.getenv('processed_container')+'/l1/circuits/')

# COMMAND ----------

display(spark.table('uc_f1_db.l1.circuits'))

# COMMAND ----------

dbutils.notebook.exit("\n All circuits processed")