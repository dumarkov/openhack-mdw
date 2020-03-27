# Databricks notebook source
spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', False)

# COMMAND ----------

stmt = 'vacuum unified.{table} retain 0 hours'

for t in sql('show tables in unified').collect():
  
  table= t.tableName
  sql(eval(f"f'{stmt}'"))