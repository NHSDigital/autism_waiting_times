# Databricks notebook source
# MAGIC %md
# MAGIC #Autism Statistics
# MAGIC ##Rolling 12 months
# MAGIC ###Create Main Metrics
# MAGIC 
# MAGIC This notebook produces the main metrics (excluding waiting times and DQ) for the new autism statistics measures outputs.

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

counts_metadata = main_metadata

# COMMAND ----------

for metric in counts_metadata:
  SOURCE_TABLE = counts_metadata[metric]['SOURCE_TABLE']
  AGEGROUPHIGHER = counts_metadata[metric]['AGEGROUPHIGHER']
  DATECOLUMN = counts_metadata[metric]['DATECOLUMN']
  COUNTCOLUMN = counts_metadata[metric]['COUNTCOLUMN']
  METRIC_TYPE = counts_metadata[metric]['METRIC_TYPE']
  FILTERCOLUMN = counts_metadata[metric]['FILTERCOLUMN']
  FILTERVALUE = counts_metadata[metric]['FILTERVALUE']
  spark.sql(f"DROP TABLE IF EXISTS {db_output}.data_{metric}")
  if METRIC_TYPE == 'ref_count':
    if AGEGROUPHIGHER != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*,
                           mnt.ReportingPeriodStartDate,
                           mnt.ReportingPeriodEndDate
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    CROSS JOIN {db_output}.months as mnt
                    WHERE {DATECOLUMN} between ReportingPeriodStartDate and ReportingPeriodEndDate
                      AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
                      AND (RecordEndDate is null or RecordEndDate >= ReportingPeriodEndDate)
                      AND Age_Group_Higher = '{AGEGROUPHIGHER}'""")
    else:
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*,
                           mnt.ReportingPeriodStartDate,
                           mnt.ReportingPeriodEndDate
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    CROSS JOIN {db_output}.months as mnt
                    WHERE {DATECOLUMN} between ReportingPeriodStartDate and ReportingPeriodEndDate
                      AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
                      AND (RecordEndDate is null or RecordEndDate >= ReportingPeriodEndDate)""")
  elif METRIC_TYPE == 'diag_count':
    if AGEGROUPHIGHER != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    WHERE {FILTERCOLUMN} = {FILTERVALUE}
                      AND Age_Group_Higher = '{AGEGROUPHIGHER}'""")
    else:
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    WHERE {FILTERCOLUMN} = {FILTERVALUE}""")
  elif METRIC_TYPE == 'open_pat_count':
    if AGEGROUPHIGHER != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*,
                           mnt.ReportingPeriodStartDate,
                           mnt.ReportingPeriodEndDate
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    INNER JOIN {db_output}.referral_months as mnt ON ref.UniqServReqID = mnt.UniqServReqID
                    WHERE ReferralRequestReceivedDate <= ReportingPeriodEndDate
                      AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
                      AND (RecordEndDate is null or RecordEndDate >= ReportingPeriodEndDate)
                      AND Age_Group_Higher = '{AGEGROUPHIGHER}'""")
    else:
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*,
                           mnt.ReportingPeriodStartDate,
                           mnt.ReportingPeriodEndDate
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    INNER JOIN {db_output}.referral_months as mnt ON ref.UniqServReqID = mnt.UniqServReqID
                    WHERE ReferralRequestReceivedDate <= ReportingPeriodEndDate
                      AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
                      AND (RecordEndDate is null or RecordEndDate >= ReportingPeriodEndDate)""")
  elif METRIC_TYPE == 'open_13_pat_count':
    if AGEGROUPHIGHER != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*,
                           mnt.ReportingPeriodStartDate,
                           mnt.ReportingPeriodEndDate
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    INNER JOIN {db_output}.referral_months as mnt ON ref.UniqServReqID = mnt.UniqServReqID
                    WHERE ReferralRequestReceivedDate <= ReportingPeriodEndDate
                      AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
                      AND (RecordEndDate is null or RecordEndDate >= ReportingPeriodEndDate)
                      AND Age_Group_Higher = '{AGEGROUPHIGHER}'
                      AND DATEDIFF(CASE WHEN ServDischDate<ReportingPeriodEndDate THEN ServDischDate ELSE ReportingPeriodEndDate END,ReferralRequestReceivedDate)>=91""")
    else:
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*,
                           mnt.ReportingPeriodStartDate,
                           mnt.ReportingPeriodEndDate
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    INNER JOIN {db_output}.referral_months as mnt ON ref.UniqServReqID = mnt.UniqServReqID
                    WHERE ReferralRequestReceivedDate <= ReportingPeriodEndDate
                      AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
                      AND (RecordEndDate is null or RecordEndDate >= ReportingPeriodEndDate)
                      AND DATEDIFF(CASE WHEN ServDischDate<ReportingPeriodEndDate THEN ServDischDate ELSE ReportingPeriodEndDate END,ReferralRequestReceivedDate)>=91""")
  elif METRIC_TYPE == 'contacts_count':
    if AGEGROUPHIGHER != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*,
                           mnt.ReportingPeriodStartDate,
                           mnt.ReportingPeriodEndDate
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    INNER JOIN (SELECT *
                                FROM {db_source}.MHS201CareContact
                                WHERE AttendOrDNACode in ('5','6')
                                  AND ((ConsMechanismMH in ('01','03') AND UniqMonthID < '1459')
                                        OR
                                        (ConsMechanismMH in ('01','11') AND UniqMonthID >= '1459'))
                                ) AS Cont
                       ON ref.Person_ID = Cont.Person_ID
                      AND ref.UniqServReqID = Cont.UniqServReqID
                    CROSS JOIN {db_output}.months as mnt
                    WHERE ReferralRequestReceivedDate <= ReportingPeriodEndDate
                      AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
                      AND (RecordEndDate is null or RecordEndDate >= ReportingPeriodEndDate)
                      AND ReferralRequestReceivedDate <= CareContDate
                      AND CareContDate between ReportingPeriodStartDate and ReportingPeriodEndDate
                      AND Age_Group_Higher = '{AGEGROUPHIGHER}'""")
    else:
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*,
                           mnt.ReportingPeriodStartDate,
                           mnt.ReportingPeriodEndDate
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    INNER JOIN (SELECT *
                                FROM {db_source}.MHS201CareContact
                                WHERE AttendOrDNACode in ('5','6')
                                  AND ((ConsMechanismMH in ('01','03') AND UniqMonthID < '1459')
                                        OR
                                        (ConsMechanismMH in ('01','11') AND UniqMonthID >= '1459'))
                                ) AS Cont
                       ON ref.Person_ID = Cont.Person_ID
                      AND ref.UniqServReqID = Cont.UniqServReqID
                    CROSS JOIN {db_output}.months as mnt
                    WHERE ReferralRequestReceivedDate <= ReportingPeriodEndDate
                      AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
                      AND (RecordEndDate is null or RecordEndDate >= ReportingPeriodEndDate)
                      AND ReferralRequestReceivedDate <= CareContDate
                      AND CareContDate between ReportingPeriodStartDate and ReportingPeriodEndDate""")
  elif METRIC_TYPE == 'ref_diag_count':
    if AGEGROUPHIGHER != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*,
                           mnt.ReportingPeriodStartDate,
                           mnt.ReportingPeriodEndDate
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    CROSS JOIN {db_output}.months as mnt
                    INNER JOIN {db_output}.Diagnoses AS diag
                       ON ref.Person_ID = diag.Person_ID
                      AND ref.UniqServReqID = diag.UniqServReqID
                      AND mnt.UniqMonthID = diag.UniqMonthID
                      AND diag.diag = 'Autism'
                    WHERE ReferralRequestReceivedDate <= mnt.ReportingPeriodEndDate
                      AND (ServDischDate is null or ServDischDate >= mnt.ReportingPeriodStartDate)
                      AND (RecordEndDate is null or RecordEndDate >= mnt.ReportingPeriodEndDate)
                      AND Age_Group_Higher = '{AGEGROUPHIGHER}'""")
    else:
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*,
                           mnt.ReportingPeriodStartDate,
                           mnt.ReportingPeriodEndDate
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    CROSS JOIN {db_output}.months as mnt
                    INNER JOIN {db_output}.Diagnoses AS diag
                       ON ref.Person_ID = diag.Person_ID
                      AND ref.UniqServReqID = diag.UniqServReqID
                      AND mnt.UniqMonthID = diag.UniqMonthID
                      AND diag.diag = 'Autism'
                    WHERE ReferralRequestReceivedDate <= mnt.ReportingPeriodEndDate
                      AND (ServDischDate is null or ServDischDate >= mnt.ReportingPeriodStartDate)
                      AND (RecordEndDate is null or RecordEndDate >= mnt.ReportingPeriodEndDate)""")

# COMMAND ----------

# spark.sql(f"TRUNCATE TABLE {db_output}.asd_output")

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
#     namecol = geog_breakdown_metadata[geog_breakdown]['namecol']
#     spellscol = geog_breakdown_metadata[geog_breakdown]['spellscol']
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

# '''Tidy up source data tables'''

# for metric in counts_metadata:
#   spark.sql(f"""DROP TABLE IF EXISTS {db_output}.data_{metric}""")