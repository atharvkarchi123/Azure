# Databricks notebook source
client_id=dbutils.secrets.get(scope='formula1-scope', key='f1-clientId-sp')
tenant_id=dbutils.secrets.get(scope='formula1-scope', key='f1-tenantId-sp')
client_secret=dbutils.secrets.get(scope='formula1-scope', key='f1-adls-client-secret-sp')

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
    source='abfss://demo@atharvkarchiadls.dfs.core.windows.net/',
    mount_point='/mnt/atharvkarchiadls/demo',
    extra_configs=configs
)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(spark.read.csv(circuits.csv, header=True, inferSchema=True))