# Databricks notebook source
import os

# COMMAND ----------

# MAGIC %run "/Workspace/Formula 1/Ingestion/utils"

# COMMAND ----------

dbutils.widgets.text('Partition','')

# COMMAND ----------

partition=dbutils.widgets.get('Partition')

# COMMAND ----------

from pyspark.sql.functions import col,concat,lit, current_timestamp

# COMMAND ----------

# DBTITLE 1,Drivers Dataframe
drivers_df=spark.table('uc_f1_db.l1.drivers').withColumnsRenamed({'first_name':'driver_first_name','last_name':'driver_last_name','number':'driver_nbr','nationality' : 'driver_nationality','code':'driver_code'}).drop('ingestion_date','source')

# COMMAND ----------

# DBTITLE 1,Constructors Dataframe
constructors_df=spark.table('uc_f1_db.l1.constructors').withColumnsRenamed({'name':'constructor_name','nationality' : 'constructor_nationality'}).drop('ingestion_date','source')

# COMMAND ----------

# DBTITLE 1,Races Dataframe
races_df=spark.table('uc_f1_db.l1.races').where(col('partition')==partition).withColumnsRenamed({'name':'race_name','date':'race_date','time' : 'race_time'}).drop('ingestion_date','source','partition')

# COMMAND ----------

# DBTITLE 1,Circuits Dataframe
circuits_df=spark.table('uc_f1_db.l1.circuits').withColumnsRenamed({'name':'circuit_name','location' : 'circuit_location','country'  :'circuit_country'}).drop('ingestion_date','source')

# COMMAND ----------

# DBTITLE 1,Sprint Results Dataframe
sprint_results_df=spark.table('uc_f1_db.l0.sprint_results').select('sprint_id','race_id','driver_id','constructor_id','sprint_points','sprint_position','sprint_grid')

# COMMAND ----------

# DBTITLE 1,Race Results Dataframe
race_results_df=spark.table('uc_f1_db.l1.results').where(col('partition')==partition).withColumnsRenamed({'time':'completed_time','number' : 'driver_nbr'}).drop('ingestion_date','source','partition')

# COMMAND ----------

race_results_df = race_results_df.join(races_df, race_results_df['race_id'] == races_df['race_id'], 'left').drop(races_df['race_id'])

# COMMAND ----------

race_results_df=race_results_df.join(circuits_df,circuits_df['circuit_id']==race_results_df['circuit_id'],'left').drop(circuits_df['circuit_id'])

# COMMAND ----------

race_results_df=race_results_df.join(drivers_df,drivers_df['driver_id']==race_results_df['driver_id'],'left').drop(drivers_df['driver_id'],race_results_df['driver_nbr'])

# COMMAND ----------

race_results_df=race_results_df.join(constructors_df,constructors_df['constructor_id']==race_results_df['constructor_id'],'left').drop(constructors_df['constructor_id'])

# COMMAND ----------

race_results_df=race_results_df\
    .join(sprint_results_df,(race_results_df['race_id']==sprint_results_df['race_id']) & (race_results_df['driver_id']==sprint_results_df['driver_id']) & (race_results_df['constructor_id']==sprint_results_df['constructor_id']),'left')\
    .drop(sprint_results_df['race_id'],sprint_results_df['driver_id'],sprint_results_df['constructor_id'])

# COMMAND ----------

race_results_df=race_results_df\
      .select(
        col('result_id'),\
        col('race_id'),\
        col('driver_id'),\
        col('constructor_id'),\
        col('sprint_id'),\
        col('position_text').alias('pos'),\
        col('driver_code').alias('driver'),\
        concat(col('forename'),lit(' '),col('surname')).alias('name'),\
        col('driver_nbr').alias('nbr'),\
        col('driver_nationality').alias('nationality'),\
        col('constructor_name').alias('constructor'),\
        col('constructor_nationality'),\
        col('points'),\
        col('race_year').alias('year'),\
        concat(col('circuit_name'),lit(' - '),col('circuit_country')).alias('circuit'),\
        col('race_name').alias('race'),\
        col('sprint_grid'),\
        col('sprint_position'),\
        col('sprint_points')
        )\
        .withColumns({'total_race_points': col('points')+col('sprint_points'),'created_at':current_timestamp(), 'partition' : lit(partition)})

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

write_table(race_results_df,'race_results','l2','src.result_id=tgt.result_id AND src.race_id=tgt.race_id AND src.driver_id=tgt.driver_id AND src.partition=tgt.partition')

# COMMAND ----------

spark.table('uc_f1_db.l2.race_results').display()

# COMMAND ----------

spark.table('uc_f1_db.l2.race_results').count()