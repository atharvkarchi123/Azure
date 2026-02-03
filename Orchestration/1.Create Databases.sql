-- Databricks notebook source
-- MAGIC %python
-- MAGIC import os 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text('raw',os.getenv('raw_container')+'/l0/')
-- MAGIC dbutils.widgets.text('processed',os.getenv('processed_container')+'/l1/')
-- MAGIC dbutils.widgets.text('presentation',os.getenv('presentation_container')+'/l2/')

-- COMMAND ----------

-- DBTITLE 1,Raw
DROP DATABASE IF EXISTS uc_f1_db.L0 CASCADE;
CREATE DATABASE uc_f1_db.L0
COMMENT 'Contains all raw data required for the project'
MANAGED LOCATION '${raw}';

-- COMMAND ----------

-- DBTITLE 1,Processed
DROP DATABASE IF EXISTS uc_f1_db.L1 CASCADE;
CREATE DATABASE uc_f1_db.L1
COMMENT 'Contains all processed data required for the project'
MANAGED LOCATION '${processed}';


-- COMMAND ----------

-- DBTITLE 1,Presentation
DROP DATABASE IF EXISTS uc_f1_db.L2 CASCADE;
CREATE DATABASE uc_f1_db.L2
COMMENT 'Contains presentation data required for dashboards'
MANAGED LOCATION '${presentation}';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit('Databases Created Successfully')