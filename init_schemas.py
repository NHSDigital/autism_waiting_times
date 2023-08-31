# Databricks notebook source
db = dbutils.widgets.get("db")
assert db

dss_corporate = dbutils.widgets.get("dss_corporate")
assert dss_corporate

# COMMAND ----------

params = {'db_output': db, 'dss_corporate': dss_corporate}

print('params:', params)

# COMMAND ----------

dbutils.notebook.run('schemas/schema_master', 0, params)