# Databricks notebook source
# MAGIC %md
# MAGIC #Autism Statistics
# MAGIC ##Rolling 12 months
# MAGIC ###Create Outputs
# MAGIC 
# MAGIC This notebook produces the raw and suppressed output tables.

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# dbutils.widgets.text("db_output", "")
# dbutils.widgets.text("db_source", "")
# dbutils.widgets.text("month_id", "")
# dbutils.widgets.text("dss_corporate", "")
# dbutils.widgets.text("rp_startdate","")
# dbutils.widgets.text("rp_enddate","")
# dbutils.widgets.text("status","")
# dbutils.widgets.text("max_rp_startdate","")

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
# month_id = dbutils.widgets.get("month_id")
# dss_corporate = dbutils.widgets.get("dss_corporate")
# rp_startdate = dbutils.widgets.get("rp_startdate")
# rp_enddate = dbutils.widgets.get("rp_enddate")
status = dbutils.widgets.get("status")
max_rp_startdate = dbutils.widgets.get("max_rp_startdate")

# COMMAND ----------

# MAGIC %run ./metric_metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC UPDATE $db_output.asd_output SET SECONDARY_LEVEL = 'NONE' WHERE SECONDARY_LEVEL = 'NULL';
# MAGIC UPDATE $db_output.asd_output SET SECONDARY_LEVEL_DESCRIPTION  = 'NONE' WHERE SECONDARY_LEVEL_DESCRIPTION  = 'NULL';

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM $db_output.asd_output_raw WHERE STATUS = '$status' AND MAX_RP_STARTDATE = '$max_rp_startdate' AND DB_SOURCE = '$db_source'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO $db_output.asd_output_raw
# MAGIC select distinct a.*
# MAGIC                 ,coalesce(c.METRIC_DESCRIPTION, 'UNKNOWN')
# MAGIC                 ,coalesce(b.metric_value, 0) as METRIC_VALUE
# MAGIC                 ,'$db_source' AS DB_SOURCE
# MAGIC                 ,'$max_rp_startdate' AS MAX_RP_STARTDATE
# MAGIC from $db_output.asd_csv_master a
# MAGIC left join $db_output.asd_output b
# MAGIC       on a.REPORTING_PERIOD_START = b.REPORTING_PERIOD_START
# MAGIC       and a.breakdown = b.breakdown 
# MAGIC       and a.PRIMARY_LEVEL = b.PRIMARY_LEVEL
# MAGIC       and a.SECONDARY_LEVEL = b.SECONDARY_LEVEL 
# MAGIC       and a.metric = b.metric
# MAGIC left join $db_output.asd_metric_name_lookup c
# MAGIC    on a.METRIC = c.METRIC

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE $db_output.asd_output_raw

# COMMAND ----------

# MAGIC %md
# MAGIC ## Suppression

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM $db_output.asd_output_suppressed WHERE STATUS = '$status' AND MAX_RP_STARTDATE = '$max_rp_startdate' AND DB_SOURCE = '$db_source'

# COMMAND ----------

# DBTITLE 1,England breakdowns (all unsuppressed)
# MAGIC %sql
# MAGIC INSERT INTO $db_output.asd_output_suppressed
# MAGIC SELECT 
# MAGIC REPORTING_PERIOD_START 
# MAGIC ,REPORTING_PERIOD_END
# MAGIC ,'$status' AS STATUS
# MAGIC ,BREAKDOWN
# MAGIC ,PRIMARY_LEVEL
# MAGIC ,PRIMARY_LEVEL_DESCRIPTION
# MAGIC ,SECONDARY_LEVEL
# MAGIC ,SECONDARY_LEVEL_DESCRIPTION
# MAGIC ,METRIC
# MAGIC ,METRIC_NAME
# MAGIC ,METRIC_VALUE
# MAGIC ,DB_SOURCE
# MAGIC ,MAX_RP_STARTDATE
# MAGIC FROM $db_output.asd_output_raw
# MAGIC WHERE LEFT(BREAKDOWN,7) = 'England';
# MAGIC 
# MAGIC OPTIMIZE $db_output.asd_output_suppressed

# COMMAND ----------

for metric in output_metadata:
  metric_type = output_metadata[metric]['METRIC_TYPE']
  numerator = output_metadata[metric]['NUMERATOR']
  print('Metric: ',metric,'; Type: ',metric_type)
  if metric_type=='RATE':
    spark.sql(f"""
                INSERT INTO {db_output}.asd_output_suppressed
                select 
                a.REPORTING_PERIOD_START 
                ,a.REPORTING_PERIOD_END
                ,a.STATUS
                ,a.BREAKDOWN
                ,a.PRIMARY_LEVEL
                ,a.PRIMARY_LEVEL_DESCRIPTION
                ,a.SECONDARY_LEVEL
                ,a.SECONDARY_LEVEL_DESCRIPTION
                ,a.METRIC
                ,a.METRIC_NAME
                ,cast(case when cast(b.METRIC_VALUE as float) < 5 then '9999999'
                           else round(cast(a.METRIC_VALUE as float),0)
                           end as string) as  METRIC_VALUE
                ,a.DB_SOURCE
                ,a.MAX_RP_STARTDATE
                from (SELECT *
                      FROM {db_output}.asd_output_raw
                      WHERE METRIC = '{metric}'
                        and LEFT(BREAKDOWN,7) != 'England' 
                        and STATUS = '{status}' 
                        AND MAX_RP_STARTDATE = '{max_rp_startdate}' 
                        AND DB_SOURCE = '{db_source}') as a
                left join (SELECT *
                           FROM {db_output}.asd_output_raw
                           WHERE METRIC = '{numerator}'
                             and LEFT(BREAKDOWN,7) != 'England' 
                             and STATUS = '{status}' 
                             AND MAX_RP_STARTDATE = '{max_rp_startdate}' 
                             AND DB_SOURCE = '{db_source}') as b
                   on a.BREAKDOWN = b.BREAKDOWN
                  and a.REPORTING_PERIOD_START = b.REPORTING_PERIOD_START
                  and a.REPORTING_PERIOD_END = b.REPORTING_PERIOD_END
                  and a.PRIMARY_LEVEL = b.PRIMARY_LEVEL
                  and a.SECONDARY_LEVEL = b.SECONDARY_LEVEL
                  and a.STATUS = b.STATUS
                  and a.DB_SOURCE = b.DB_SOURCE
                  and a.MAX_RP_STARTDATE = b.MAX_RP_STARTDATE
                """)
    spark.sql(f"OPTIMIZE {db_output}.asd_output_suppressed")
  else:
    spark.sql(f"""
                INSERT INTO {db_output}.asd_output_suppressed
                select 
                a.REPORTING_PERIOD_START 
                ,a.REPORTING_PERIOD_END
                ,a.STATUS
                ,a.BREAKDOWN
                ,a.PRIMARY_LEVEL
                ,a.PRIMARY_LEVEL_DESCRIPTION
                ,a.SECONDARY_LEVEL
                ,a.SECONDARY_LEVEL_DESCRIPTION
                ,a.METRIC
                ,a.METRIC_NAME
                ,cast(case when cast(a.METRIC_VALUE as float) < 5 then '9999999'
                           else round(cast(a.METRIC_VALUE as float)/5,0)*5
                           end as string) as  METRIC_VALUE
                ,a.DB_SOURCE
                ,a.MAX_RP_STARTDATE
                from {db_output}.asd_output_raw a
                where LEFT(a.BREAKDOWN,7) != 'England'
                  and a.METRIC = '{metric}' 
                  and STATUS = '{status}' 
                  AND MAX_RP_STARTDATE = '{max_rp_startdate}' 
                  AND DB_SOURCE = '{db_source}'
                """)
    spark.sql(f"OPTIMIZE {db_output}.asd_output_suppressed")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC UPDATE $db_output.asd_output_suppressed SET METRIC_VALUE = '*' WHERE METRIC_VALUE = '9999999';

# COMMAND ----------

# DBTITLE 1,Create Main Raw Output (excluding DQ Measures)
# MAGIC %sql
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE $db_output.asd_main_output_raw
# MAGIC SELECT REPORTING_PERIOD_START 
# MAGIC       ,REPORTING_PERIOD_END
# MAGIC       ,STATUS
# MAGIC       ,BREAKDOWN
# MAGIC       ,PRIMARY_LEVEL
# MAGIC       ,PRIMARY_LEVEL_DESCRIPTION
# MAGIC       ,SECONDARY_LEVEL
# MAGIC       ,SECONDARY_LEVEL_DESCRIPTION
# MAGIC       ,METRIC
# MAGIC       ,METRIC_NAME
# MAGIC       ,METRIC_VALUE
# MAGIC FROM $db_output.asd_output_raw
# MAGIC WHERE METRIC NOT IN ('ASD11','ASD11a','ASD11b','ASD14','ASD14a','ASD14b','ASD17','ASD17a','ASD17b')
# MAGIC   AND STATUS = '$status'
# MAGIC   AND MAX_RP_STARTDATE = '$max_rp_startdate'
# MAGIC   AND DB_SOURCE = '$db_source'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE $db_output.asd_main_output
# MAGIC SELECT REPORTING_PERIOD_START 
# MAGIC       ,REPORTING_PERIOD_END
# MAGIC       ,STATUS
# MAGIC       ,BREAKDOWN
# MAGIC       ,PRIMARY_LEVEL
# MAGIC       ,PRIMARY_LEVEL_DESCRIPTION
# MAGIC       ,SECONDARY_LEVEL
# MAGIC       ,SECONDARY_LEVEL_DESCRIPTION
# MAGIC       ,METRIC
# MAGIC       ,METRIC_NAME
# MAGIC       ,METRIC_VALUE
# MAGIC FROM $db_output.asd_output_suppressed
# MAGIC WHERE METRIC NOT IN ('ASD11','ASD11a','ASD11b','ASD14','ASD14a','ASD14b','ASD17','ASD17a','ASD17b')
# MAGIC   AND STATUS = '$status'
# MAGIC   AND MAX_RP_STARTDATE = '$max_rp_startdate'
# MAGIC   AND DB_SOURCE = '$db_source'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE $db_output.asd_dq_output_raw
# MAGIC SELECT REPORTING_PERIOD_START 
# MAGIC       ,REPORTING_PERIOD_END
# MAGIC       ,STATUS
# MAGIC       ,BREAKDOWN
# MAGIC       ,PRIMARY_LEVEL
# MAGIC       ,PRIMARY_LEVEL_DESCRIPTION
# MAGIC       ,SECONDARY_LEVEL
# MAGIC       ,SECONDARY_LEVEL_DESCRIPTION
# MAGIC       ,METRIC
# MAGIC       ,METRIC_NAME
# MAGIC       ,METRIC_VALUE
# MAGIC FROM $db_output.asd_output_raw
# MAGIC WHERE METRIC IN ('ASD11','ASD11a','ASD11b','ASD14','ASD14a','ASD14b','ASD17','ASD17a','ASD17b')
# MAGIC   AND STATUS = '$status'
# MAGIC   AND MAX_RP_STARTDATE = '$max_rp_startdate'
# MAGIC   AND DB_SOURCE = '$db_source'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE $db_output.asd_dq_output
# MAGIC SELECT REPORTING_PERIOD_START 
# MAGIC       ,REPORTING_PERIOD_END
# MAGIC       ,STATUS
# MAGIC       ,BREAKDOWN
# MAGIC       ,PRIMARY_LEVEL
# MAGIC       ,PRIMARY_LEVEL_DESCRIPTION
# MAGIC       ,SECONDARY_LEVEL
# MAGIC       ,SECONDARY_LEVEL_DESCRIPTION
# MAGIC       ,METRIC
# MAGIC       ,METRIC_NAME
# MAGIC       ,METRIC_VALUE
# MAGIC FROM $db_output.asd_output_suppressed
# MAGIC WHERE METRIC IN ('ASD11','ASD11a','ASD11b','ASD14','ASD14a','ASD14b','ASD17','ASD17a','ASD17b')
# MAGIC   AND STATUS = '$status'
# MAGIC   AND MAX_RP_STARTDATE = '$max_rp_startdate'
# MAGIC   AND DB_SOURCE = '$db_source'