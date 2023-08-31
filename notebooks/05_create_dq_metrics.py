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
dss_corporate = dbutils.widgets.get("dss_corporate")
# rp_startdate = dbutils.widgets.get("rp_startdate")
# rp_enddate = dbutils.widgets.get("rp_enddate")
# status = dbutils.widgets.get("status")

# COMMAND ----------

# MAGIC %run ./metric_metadata

# COMMAND ----------

counts_metadata = dq_metadata

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
  if METRIC_TYPE == 'open_refs':
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
                    """)
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
                    """)
  elif METRIC_TYPE == 'last_refs':
    if AGEGROUPHIGHER != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*,
                           mnt.ReportingPeriodStartDate,
                           mnt.ReportingPeriodEndDate
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    INNER JOIN {db_output}.Months as mnt ON ref.UniqMonthID = mnt.UniqMonthID
                    WHERE ReferralRequestReceivedDate <= ReportingPeriodEndDate
                      AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
                      AND (RecordEndDate is null or RecordEndDate >= ReportingPeriodEndDate)
                      AND Age_Group_Higher = '{AGEGROUPHIGHER}'
                    """)
    else:
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT ref.*,
                           mnt.ReportingPeriodStartDate,
                           mnt.ReportingPeriodEndDate
                    FROM {db_output}.{SOURCE_TABLE} as ref
                    INNER JOIN {db_output}.Months as mnt ON ref.UniqMonthID = mnt.UniqMonthID
                    WHERE ReferralRequestReceivedDate <= ReportingPeriodEndDate
                      AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
                      AND (RecordEndDate is null or RecordEndDate >= ReportingPeriodEndDate)
                    """)
  else:
    if AGEGROUPHIGHER != '':
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT prep.* ,
                           mpi.Age_Group,
                           mpi.Der_Gender,
                           mpi.LowerEthnicity,
                           mpi.LowerEthnicity_Desc,
                           mpi.CCGCode,
                           mpi.CCGName,
                           mpi.STPCode,
                           mpi.STPName,
                           mpi.RegionCode,
                           mpi.RegionName,
                           mpi.Age_Group as Age_Group_Prov,
                           mpi.Der_Gender_Prov,
                           mpi.LowerEthnicity_Prov,
                           mpi.LowerEthnicity_Desc_Prov,
                           mpi.OrgIDProv,
                           '' as Name

                    FROM (SELECT   ReportingPeriodStartDate
                                  ,ReportingPeriodEndDate
                                  ,ref.UniqMonthID
                                  ,COUNT(distinct ref.UniqServReqID) as Referrals
                                  ,Person_ID
                          FROM {db_output}.{SOURCE_TABLE} as ref
                          CROSS JOIN {db_output}.months as mnt
                          LEFT JOIN ( SELECT * FROM {dss_corporate}.ORG_DAILY WHERE Org_Is_Current = '1') as org ON ref.OrgIDPRov = org.ORG_CODE
                          WHERE ReferralRequestReceivedDate <= ReportingPeriodEndDate
                            AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
                            AND Age_Group_Higher = '{AGEGROUPHIGHER}'
                          GROUP BY ReportingPeriodStartDate
                                  ,ReportingPeriodEndDate
                                  ,ref.UniqMonthID
                                  ,Person_ID
                          HAVING COUNT(DISTINCT ref.UniqServReqID) > 1) as prep
                    LEFT JOIN {db_output}.{SOURCE_TABLE} as mpi
                       ON prep.UniqMonthID = mpi.UniqMonthID
                      AND prep.Person_ID = mpi.Person_ID
                    """)
    else:
      spark.sql(f"""CREATE TABLE IF NOT EXISTS {db_output}.data_{metric} USING DELTA AS
                    SELECT prep.* ,
                           mpi.Age_Group,
                           mpi.Der_Gender,
                           mpi.LowerEthnicity,
                           mpi.LowerEthnicity_Desc,
                           mpi.CCGCode,
                           mpi.CCGName,
                           mpi.STPCode,
                           mpi.STPName,
                           mpi.RegionCode,
                           mpi.RegionName,
                           mpi.Age_Group as Age_Group_Prov,
                           mpi.Der_Gender_Prov,
                           mpi.LowerEthnicity_Prov,
                           mpi.LowerEthnicity_Desc_Prov,
                           mpi.OrgIDProv,
                           '' as Name

                    FROM (SELECT   ReportingPeriodStartDate
                                  ,ReportingPeriodEndDate
                                  ,ref.UniqMonthID
                                  ,COUNT(distinct ref.UniqServReqID) as Referrals
                                  ,Person_ID
                          FROM {db_output}.{SOURCE_TABLE} as ref
                          CROSS JOIN {db_output}.months as mnt
                          LEFT JOIN ( SELECT * FROM {dss_corporate}.ORG_DAILY WHERE Org_Is_Current = '1') as org ON ref.OrgIDPRov = org.ORG_CODE
                          WHERE ReferralRequestReceivedDate <= ReportingPeriodEndDate
                            AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
                          GROUP BY ReportingPeriodStartDate
                                  ,ReportingPeriodEndDate
                                  ,ref.UniqMonthID
                                  ,Person_ID
                          HAVING COUNT(DISTINCT ref.UniqServReqID) > 1) as prep
                    LEFT JOIN {db_output}.{SOURCE_TABLE} as mpi
                       ON prep.UniqMonthID = mpi.UniqMonthID
                      AND prep.Person_ID = mpi.Person_ID
                    """)

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

# '''Tidy up source data tables'''

# for metric in counts_metadata:
#   spark.sql(f"""DROP TABLE IF EXISTS {db_output}.data_{metric}""")
#   spark.sql(f"""DROP TABLE IF EXISTS {db_output}.data_{metric}_Prov""")