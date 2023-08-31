# Databricks notebook source
# COMMAND ----------

'''code promotion project parameters - these are part of the set up of the project and will ALWAYS be available at run time'''

dss_corporate = dbutils.widgets.get("dss_corporate")

# the target database for a cp project is the same as the name of the cp project and the parameter is always named db
# here this is renamed to the standard db_output for familiarity/consistency with other projects
db_output = dbutils.widgets.get("db")
print(db_output)
assert db_output

# the parameter name of any source database(s) for a cp project is the same as the database name, i.e. the mh_v5_pre_clear parameter is named mh_v5_pre_clear!
# here this is renamed to the standard db_source for familiarity/consistency with other projects
db_source = dbutils.widgets.get("mh_v5_pre_clear")
print(db_source)
assert db_source

# COMMAND ----------

'''additional parameters - these need to be fed in at run time as additional parameters in order to be available'''
 

new_metrics = dbutils.widgets.get("new_metrics")
print(new_metrics)
# assert new_metrics

max_rp_startdate = dbutils.widgets.get("max_rp_startdate")
print(max_rp_startdate)
assert max_rp_startdate

# here a parameter is defined and assigned no value - this makes it an optional parameter - it doesn't need to be added at run time because it will be added here with no value if it doesn't already exist.
# this avoids the following error which you'd see with the params above if you forgot to add them at runtime
# InputWidgetNotDefined: No input widget named product is defined
# a single product can be run by adding this to the parameter list at run time

dbutils.widgets.text("product","","product")

if dbutils.widgets.get("product") == '':
  product = "ALL"
else:
  product = dbutils.widgets.get("product")
print(product)

# COMMAND ----------

# Added for alternative run with different source data

dbutils.widgets.text("alt_source_data","","alt_source_data")
alt_source_data = dbutils.widgets.get("alt_source_data")

print("alt_source_data: ",alt_source_data)


if len(alt_source_data)>0:
  db_source = dbutils.widgets.get("alt_source_data")

# COMMAND ----------

# Check database has data for latest month
from datetime import datetime
header_max_rp_startdate = spark.sql(f"SELECT MAX(ReportingPeriodStartDate) FROM {db_source}.mhs000header").collect()[0][0]
print("Max Startdate in Header: ",header_max_rp_startdate)
dt_max_rp_startdate = datetime.strptime(max_rp_startdate, '%Y-%m-%d').date()
print("Max Startdate in Widget: ",dt_max_rp_startdate)
assert header_max_rp_startdate >= dt_max_rp_startdate, "Data for selected time period not available."

# COMMAND ----------

''' This cell defines the monthidend widget based on max_rp_startdate'''
monthidend = spark.sql("SELECT MAX(UniqMonthID) as UniqMonthID from {db_source}.mhs000header WHERE ReportingPeriodStartDate = '{max_rp_startdate}'".format(db_source=db_source,max_rp_startdate=max_rp_startdate)).collect()[0]['UniqMonthID']
print('monthidend:',monthidend)

# COMMAND ----------

''' This cell defines the monthidstart widget depending on whether new metrics are included in the run
    If there are no new metrics, a 12 month run is completed. If there are new metrics, the run covers
    all months from April 2019 (1429)'''

if new_metrics == "":
  monthidstart = str(int(monthidend)-11)
else:
  monthidstart = "1429"
  
monthidstart

# COMMAND ----------

startdate = spark.sql("SELECT MIN(ReportingPeriodStartDate) as startdate from {db_source}.mhs000header WHERE UniqMonthID = '{monthidstart}'".format(db_source=db_source,monthidstart=monthidstart)).collect()[0]['startdate']
print('startdate:',startdate)

enddate = spark.sql("SELECT MAX(ReportingPeriodEndDate) as enddate from {db_source}.mhs000header WHERE UniqMonthID = '{monthidend}'".format(db_source=db_source,monthidend=monthidend)).collect()[0]['enddate']
print('enddate:',enddate)

status = 'Latest Available Data'

# COMMAND ----------

params = {'db_source': db_source, 'db_output' : db_output, 'rp_enddate': str(enddate), 'rp_startdate': str(startdate), 'month_id': monthidend, 'monthidstart': monthidstart, 'status': status, 'dss_corporate': dss_corporate, 'max_rp_startdate': str(max_rp_startdate), 'new_metrics': new_metrics};
print(params)

# COMMAND ----------

dbutils.notebook.run("./notebooks/01_create_generic_prep_tables", 0, params )

# COMMAND ----------

dbutils.notebook.run("./notebooks/02_create_master_reference_data", 0, params )

# COMMAND ----------

dbutils.notebook.run("./notebooks/03_create_main_metrics", 0, params )

# COMMAND ----------

dbutils.notebook.run("./notebooks/04_create_waiting_time_metrics", 0, params )

# COMMAND ----------

dbutils.notebook.run("./notebooks/05_create_dq_metrics", 0, params )

# COMMAND ----------

dbutils.notebook.run("./notebooks/06_create_outputs", 0, params )

# COMMAND ----------

from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import time
import os
import json

if new_metrics:
  fy_dates = spark.sql("SELECT MIN(ReportingPeriodStartDate) as rp_start, max(ReportingPeriodEndDate) as rp_end, OutputFile as FY FROM {db_output}.Months GROUP BY OutputFile ORDER BY 3".format(db_output=db_output)).collect()
  for rp_start, rp_end, FY in fy_dates:
    extract_params = {'db_source': db_source, 'db_output' : db_output, 'rp_enddate': str(rp_end), 'rp_startdate': str(rp_start), 'month_id': monthidend, 'monthidstart': monthidstart, 'status': status, 'dss_corporate': dss_corporate, 'max_rp_startdate': str(max_rp_startdate), 'new_metrics': new_metrics, 'FY': FY};
    print(extract_params)
    dbutils.notebook.run("./notebooks/07_autism_waiting_times_extract", 0, extract_params )
else:
  dbutils.notebook.run("./notebooks/07_autism_waiting_times_extract", 0, params )
print(f'autism_waiting_times_extract\n')

# COMMAND ----------

dbutils.notebook.run("./notebooks/99_truncate_tables", 0, params )