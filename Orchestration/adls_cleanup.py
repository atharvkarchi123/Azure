# Databricks notebook source
dbutils.widgets.text('file','')

# COMMAND ----------

dbutils.fs.rm(dbutils.widgets.get('file'))