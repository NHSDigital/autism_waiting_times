# Databricks notebook source
# dbutils.widgets.removeAll()

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")
print(db_output)
assert db_output

dss_corporate = dbutils.widgets.get("dss_corporate")
assert dss_corporate

# COMMAND ----------

params = {'db_output': db_output, 'dss_corporate': dss_corporate}

print('params:', params)

# COMMAND ----------

dbutils.notebook.run('01_create_common_tables', 0, params)

# COMMAND ----------

dbutils.notebook.run('02_load_reference_data', 0, params)

# COMMAND ----------

dbutils.notebook.run('03_validcodes', 0, params)
