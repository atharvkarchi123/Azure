# Databricks notebook source
# MAGIC %md
# MAGIC #Getting secret parameters 

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='formula1-scope')

# COMMAND ----------

access_key=dbutils.secrets.get(scope='formula1-scope', key='accessKey-f1-adls')

# COMMAND ----------

# MAGIC %md
# MAGIC #Access Key

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.atharvkarchiadls.dfs.core.windows.net",
   access_key)

# COMMAND ----------

display(dbutils.fs.ls('abfs://demo@atharvkarchiadls.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@atharvkarchiadls.dfs.core.windows.net/circuits.csv', header=True))

# COMMAND ----------

# MAGIC %md
# MAGIC #SAS Token

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.atharvkarchiadls.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.atharvkarchiadls.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.atharvkarchiadls.dfs.core.windows.net", "sp=rl&st=2026-01-06T12:48:34Z&se=2026-01-06T16:03:34Z&spr=https&sv=2024-11-04&sr=c&sig=8zLnKUp8q9gBaL0lHSkRM35fKOr3GpJiGSarYCV0WPY%3D")


# COMMAND ----------

display(dbutils.fs.ls('abfs://demo@atharvkarchiadls.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@atharvkarchiadls.dfs.core.windows.net/circuits.csv', header=True))

# COMMAND ----------

# MAGIC %md
# MAGIC #Service Principle

# COMMAND ----------

client_id=dbutils.secrets.get(scope='formula1-scope', key='f1-clientId-sp')
tenant_id=dbutils.secrets.get(scope='formula1-scope', key='f1-tenantId-sp')
client_secret=dbutils.secrets.get(scope='formula1-scope', key='f1-adls-client-secret-sp')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

display(dbutils.fs.ls('abfs://demo@atharvkarchiadls.dfs.core.windows.net'))

# COMMAND ----------

display(spark.read.csv('abfss://demo@atharvkarchiadls.dfs.core.windows.net/circuits.csv',header=True))