# Databricks notebook source
from delta.tables import DeltaTable
spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.sql("USE CATALOG uc_f1_db")

# COMMAND ----------

def does_table_exist(table_name,schema='l1'):
    tables = spark.catalog.listTables(schema, "uc_f1_db")
    table_names = [t.name for t in tables]
    if table_name in table_names:
        return True
    else:
        return False


# COMMAND ----------

def write_table(df, table_name,schema='l1',merge_condition=''):
    if does_table_exist(table_name,schema):
        table =DeltaTable.forName(spark, f"uc_f1_db.{schema}.{table_name}")
        table.alias('tgt').merge(
            df.alias('src'),
            merge_condition
        )\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    else:
        df.write.mode("overwrite").partitionBy('partition')\
            .saveAsTable(f"uc_f1_db.{schema}.{table_name}",path=os.getenv('processed_container')+f'/l1/{table_name}' if schema=='l1' else os.getenv('presentation_container')+f'/l2/{table_name}')