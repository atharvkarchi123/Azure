-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text('circuits','')
-- MAGIC dbutils.widgets.text('constructors','')
-- MAGIC dbutils.widgets.text('drivers','')
-- MAGIC dbutils.widgets.text('sprint_results','')

-- COMMAND ----------

-- DBTITLE 1,Circuits
DROP TABLE IF EXISTS uc_f1_db.l0.CIRCUITS;

CREATE TABLE uc_f1_db.l0.CIRCUITS(
  CIRCUIT_ID INT NOT NULL,
  CIRCUIT_REF STRING,
  NAME STRING,
  LOCATION STRING,
  COUNTRY STRING,
  LATITUDE DOUBLE,
  LONGITUDE DOUBLE,
  ALTITUDE INT,
  URL STRING
)
USING CSV
OPTIONS (header = 'true')
LOCATION '${circuits}';

-- COMMAND ----------

-- DBTITLE 1,Constructors
DROP TABLE IF EXISTS uc_f1_db.l0.CONSTRUCTORS;
CREATE TABLE uc_f1_db.l0.CONSTRUCTORS(
constructorId INT,
constructorRef STRING, 
name STRING, 
nationality STRING, 
url STRING
)
USING CSV
OPTIONS (header = 'true')
LOCATION '${constructors}';


-- COMMAND ----------

-- DBTITLE 1,Drivers
DROP TABLE IF EXISTS uc_f1_db.l0.DRIVERS;
CREATE TABLE uc_f1_db.l0.DRIVERS(
    driverId INT NOT NULL,
    driverRef STRING,
    number INT,
    code STRING,
    forename STRING,
    surname STRING,
    dob DATE,
    nationality STRING,
    url STRING
)
USING CSV
OPTIONS (header = 'true')
LOCATION '${drivers}';

-- COMMAND ----------

DROP TABLE IF EXISTS uc_f1_db.L0.SPRINT_RESULTS;
CREATE TABLE IF NOT EXISTS uc_f1_db.L0.SPRINT_RESULTS(
  sprint_id INT,
  race_Id INT,
  driver_Id INT,
  constructor_Id INT,
  number INT,
  sprint_grid INT,
  sprint_position INT,
  positionText STRING,
  positionOrder INT,
  sprint_points INT,
  laps INT,
  time STRING,
  milliseconds INT
)
USING CSV
OPTIONS (header = 'true')
LOCATION '${sprint_results}'