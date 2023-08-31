# Databricks notebook source
# MAGIC %md
# MAGIC #Autism Statistics
# MAGIC ##Rolling 12 months
# MAGIC ###Create Waiting Time Metrics
# MAGIC 
# MAGIC This notebook produces the waiting time metrics for the autism statistics measures outputs.

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

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
# month_id = dbutils.widgets.get("month_id")
# dss_corporate = dbutils.widgets.get("dss_corporate")
# rp_startdate = dbutils.widgets.get("rp_startdate")
# rp_enddate = dbutils.widgets.get("rp_enddate")
# status = dbutils.widgets.get("status")

# COMMAND ----------

# MAGIC %run ./metric_metadata

# COMMAND ----------

counts_metadata = waits_metadata

# COMMAND ----------

for metric in counts_metadata:
  SOURCE_TABLE = counts_metadata[metric]['SOURCE_TABLE']
  AGEGROUPHIGHER = counts_metadata[metric]['AGEGROUPHIGHER']
  COUNTCOLUMN = counts_metadata[metric]['COUNTCOLUMN']
  METRIC_TYPE = counts_metadata[metric]['METRIC_TYPE']
  FILTERCOLUMN = counts_metadata[metric]['FILTERCOLUMN']
  FILTERVALUE = counts_metadata[metric]['FILTERVALUE']
  spark.sql(f"DROP TABLE IF EXISTS {db_output}.data_{metric}")
  if METRIC_TYPE == 'not':
    if AGEGROUPHIGHER != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    WHERE {FILTERCOLUMN} != '{FILTERVALUE}'
                      AND Wait13WeeksPlus = 'Y'
                      AND DATEDIFF(CASE WHEN ServDischDate<ReportingPeriodEndDate THEN ServDischDate ELSE ReportingPeriodEndDate END,ReferralRequestReceivedDate)>=91
                      AND Age_Group_Higher = '{AGEGROUPHIGHER}'""")
    else:
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    WHERE {FILTERCOLUMN} != '{FILTERVALUE}'
                      AND Wait13WeeksPlus = 'Y'
                      AND DATEDIFF(CASE WHEN ServDischDate<ReportingPeriodEndDate THEN ServDischDate ELSE ReportingPeriodEndDate END,ReferralRequestReceivedDate)>=91""")
  else:
    if AGEGROUPHIGHER != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    WHERE {FILTERCOLUMN} IN ('{FILTERVALUE}')
                      AND DATEDIFF(CASE WHEN ServDischDate<ReportingPeriodEndDate THEN ServDischDate ELSE ReportingPeriodEndDate END,ReferralRequestReceivedDate)>=91
                      AND Age_Group_Higher = '{AGEGROUPHIGHER}'""")
    else:
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    WHERE {FILTERCOLUMN} IN ('{FILTERVALUE}')
                      AND DATEDIFF(CASE WHEN ServDischDate<ReportingPeriodEndDate THEN ServDischDate ELSE ReportingPeriodEndDate END,ReferralRequestReceivedDate)>=91""")

# COMMAND ----------

for metric in counts_metadata:
  COUNTCOLUMN = counts_metadata[metric]['COUNTCOLUMN']
  for breakdown in breakdowns_metadata:
    name = breakdowns_metadata[breakdown]['name']
    col = breakdowns_metadata[breakdown]['column']
    spark.sql(f"""  INSERT INTO {db_output}.asd_output

                    SELECT 
                    ReportingPeriodStartDate AS REPORTING_PERIOD_START,
                    ReportingPeriodEndDate AS REPORTING_PERIOD_END,
                    'Latest Available' AS STATUS,
                    '{name}' AS BREAKDOWN,
                    COALESCE({col},'UNKNOWN') AS PRIMARY_LEVEL,
                    'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
                    'NULL' AS SECONDARY_LEVEL,
                    'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
                    '{metric}' AS METRIC,
                    COALESCE(COUNT(DISTINCT {COUNTCOLUMN}),0) AS METRIC_VALUE,
                    '{db_source}' AS SOURCE_DB
                    FROM 
                    {db_output}.data_{metric}
                    GROUP BY ReportingPeriodStartDate, ReportingPeriodEndDate, COALESCE({col},'UNKNOWN')""")
spark.sql(f"OPTIMIZE {db_output}.asd_output ZORDER BY (Metric, Breakdown)")

# COMMAND ----------

for metric in counts_metadata:
  COUNTCOLUMN = counts_metadata[metric]['COUNTCOLUMN']
  for geog_breakdown in geog_breakdown_metadata:
    geogname = geog_breakdown_metadata[geog_breakdown]['name']
    codecol = geog_breakdown_metadata[geog_breakdown]['codecol']
    demogsuffix = geog_breakdown_metadata[geog_breakdown]['demogsuffix']
    for breakdown in breakdowns_metadata:
      if breakdown == 'AGE_LL':
        name = geogname+'; '+breakdowns_metadata[breakdown]['name']
        col = breakdowns_metadata[breakdown]['column']
      elif breakdown != 'England':
        name = geogname+'; '+breakdowns_metadata[breakdown]['name']
        col = breakdowns_metadata[breakdown]['column']+demogsuffix
      else:
        name = geogname
        col = '"NULL"'
      spark.sql(f"""  INSERT INTO {db_output}.asd_output

                      SELECT 
                      ReportingPeriodStartDate AS REPORTING_PERIOD_START,
                      ReportingPeriodEndDate AS REPORTING_PERIOD_END,
                      'Latest Available' AS STATUS,
                      '{name}' AS BREAKDOWN,
                      COALESCE({codecol},'UNKNOWN') AS PRIMARY_LEVEL,
                      'NULL' AS PRIMARY_LEVEL_DESCRIPTION,
                      COALESCE({col},'UNKNOWN') AS SECONDARY_LEVEL,
                      'NULL' AS SECONDARY_LEVEL_DESCRIPTION,
                      '{metric}' AS Metric,
                      COALESCE(COUNT(DISTINCT {COUNTCOLUMN}),0) AS Metric_Value,
                      '{db_source}' AS SOURCE_DB
                      FROM 
                      {db_output}.data_{metric}
                      GROUP BY ReportingPeriodStartDate, ReportingPeriodEndDate, 
                      COALESCE({codecol},'UNKNOWN'),
                      COALESCE({col},'UNKNOWN')""")
spark.sql(f"OPTIMIZE {db_output}.asd_output ZORDER BY (Metric, Breakdown)")

# COMMAND ----------

'''Calculate ASD19 Proportions from ASD18 and ASD16 outputs'''
for metric in props_metadata:
  NUMERATOR = props_metadata[metric]['NUMERATOR']
  DENOMINATOR = props_metadata[metric]['DENOMINATOR']
  spark.sql(f"""INSERT INTO {db_output}.asd_output
                SELECT a.REPORTING_PERIOD_START,
                       a.REPORTING_PERIOD_END,
                       a.STATUS,
                       a.BREAKDOWN,
                       a.PRIMARY_LEVEL,
                       a.PRIMARY_LEVEL_DESCRIPTION,
                       a.SECONDARY_LEVEL,
                       a.SECONDARY_LEVEL_DESCRIPTION,
                       CASE WHEN b.Metric = 'ASD18' THEN 'ASD19' ELSE CONCAT('ASD19',LOWER(RIGHT(b.Metric,1))) END AS METRIC,
                       COALESCE(b.METRIC_VALUE,0)/a.METRIC_VALUE*100 AS METRIC_VALUE,
                       a.SOURCE_DB 
                FROM
                    (
                    SELECT *
                    FROM {db_output}.asd_output
                    WHERE Metric = '{NUMERATOR}'
                    ) as b
                INNER JOIN
                    (
                    SELECT *
                    FROM {db_output}.asd_output
                    WHERE Metric = '{DENOMINATOR}'
                    ) as a
                   ON a.Reporting_Period_Start = b.REPORTING_PERIOD_START
                  AND a.BREAKDOWN = b.BREAKDOWN
                  AND a.PRIMARY_LEVEL = b.PRIMARY_LEVEL
                  AND a.SECONDARY_LEVEL = b.SECONDARY_LEVEL
                ORDER BY REPORTING_PERIOD_START""")
  spark.sql(f"OPTIMIZE {db_output}.asd_output ZORDER BY (Metric, Breakdown)")

# COMMAND ----------

# '''Tidy up source data tables'''

# for metric in counts_metadata:
#   spark.sql(f"""DROP TABLE IF EXISTS {db_output}.data_{metric}""")