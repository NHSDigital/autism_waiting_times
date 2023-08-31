# Databricks notebook source
# MAGIC %md
# MAGIC #Autism Statistics
# MAGIC ##Rolling 12 months
# MAGIC ###Reference Data Load Notebook
# MAGIC 
# MAGIC This notebook produces the reference data tables for the new autism statistics measures outputs.

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
month_id = dbutils.widgets.get("month_id")
dss_corporate = dbutils.widgets.get("dss_corporate")
rp_startdate = dbutils.widgets.get("rp_startdate")
rp_enddate = dbutils.widgets.get("rp_enddate")
status = dbutils.widgets.get("status")

# COMMAND ----------

# MAGIC %sql
# MAGIC ---------------NOT CURRENTLY USED AS NO STP OR REGION BREAKDOWNS ARE PRODUCED
# MAGIC -- CREATE OR REPLACE TEMPORARY VIEW asd_stp_mapping AS 
# MAGIC -- SELECT A.ORG_cODE as STP_CODE, 
# MAGIC --        A.NAME as STP_NAME, 
# MAGIC --        C.ORG_CODE as CCG_CODE, 
# MAGIC --        C.NAME as CCG_NAME,
# MAGIC --        E.ORG_CODE as REGION_CODE,
# MAGIC --        E.NAME as REGION_NAME
# MAGIC -- FROM $db_output.asd_org_daily A
# MAGIC -- LEFT JOIN $db_output.asd_org_relationship_daily B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
# MAGIC -- LEFT JOIN $db_output.asd_org_daily C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
# MAGIC -- LEFT JOIN $db_output.asd_org_relationship_daily D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
# MAGIC -- LEFT JOIN $db_output.asd_org_daily E ON D.REL_TO_ORG_CODE = E.ORG_CODE
# MAGIC -- WHERE A.ORG_TYPE_CODE = 'ST'
# MAGIC --   AND B.REL_TYPE_CODE is not null
# MAGIC -- ORDER BY 1

# COMMAND ----------

# DBTITLE 1,Populating asd_ref_master1
# MAGIC %sql
# MAGIC 
# MAGIC TRUNCATE TABLE $db_output.asd_ref_master1;
# MAGIC INSERT INTO $db_output.asd_ref_master1 VALUES
# MAGIC -- England --
# MAGIC ('England', 'England', 'England'),
# MAGIC -- CCG - GP Practice or Residence --
# MAGIC ('CCG or Sub ICB Location - GP Practice or Residence','UNKNOWN', 'UNKNOWN'),
# MAGIC -- STP --
# MAGIC ('STP or ICB - GP Practice or Residence','UNKNOWN', 'UNKNOWN'),
# MAGIC -- Region --
# MAGIC ('Commissioning Region - GP Practice or Residence','UNKNOWN', 'UNKNOWN'),
# MAGIC -- AGE_HL --
# MAGIC ('AGE_HL','Under 18', 'People aged under 18'),
# MAGIC ('AGE_HL','18 and over', 'People aged 18 or over'),
# MAGIC ('AGE_HL','UNKNOWN', 'UNKNOWN'),
# MAGIC -- AGE_LL --
# MAGIC ('AGE_LL','Under 10', 'People aged under 10'),
# MAGIC ('AGE_LL','10 to 17', 'People aged 10 to 17'),
# MAGIC ('AGE_LL','18 to 24', 'People aged 18 to 24'),
# MAGIC ('AGE_LL','25 to 34', 'People aged 25 to 34'),
# MAGIC ('AGE_LL','35 to 44', 'People aged 35 to 44'),
# MAGIC ('AGE_LL','45 to 54', 'People aged 45 to 54'),
# MAGIC ('AGE_LL','55 to 64', 'People aged 55 to 64'),
# MAGIC ('AGE_LL','65 and Over', 'People aged 65 or over'),
# MAGIC ('AGE_LL','UNKNOWN', 'UNKNOWN'),
# MAGIC -- GENDER --
# MAGIC ('GENDER','1', 'Male'),
# MAGIC ('GENDER','2', 'Female'),
# MAGIC ('GENDER','3', 'Non-Binary'),
# MAGIC ('GENDER','4', 'Other (not listed)'),
# MAGIC ('GENDER','9', 'Indeterminate'),
# MAGIC ('GENDER','UNKNOWN', 'UNKNOWN'),
# MAGIC -- ETHNICITY --
# MAGIC ('L_ETH','A', 'British'),
# MAGIC ('L_ETH','B', 'Irish'),
# MAGIC ('L_ETH','C', 'Any Other White Background'),
# MAGIC ('L_ETH','D', 'White and Black Caribbean'),
# MAGIC ('L_ETH','E', 'White and Black African'),
# MAGIC ('L_ETH','F', 'White and Asian'),
# MAGIC ('L_ETH','G', 'Any Other Mixed Background'),
# MAGIC ('L_ETH','H', 'Indian'),
# MAGIC ('L_ETH','J', 'Pakistani'),
# MAGIC ('L_ETH','K', 'Bangladeshi'),
# MAGIC ('L_ETH','L', 'Any Other Asian Background'),
# MAGIC ('L_ETH','M', 'Caribbean'),
# MAGIC ('L_ETH','N', 'African'),
# MAGIC ('L_ETH','P', 'Any Other Black Background'),
# MAGIC ('L_ETH','R', 'Chinese'),
# MAGIC ('L_ETH','S', 'Any other ethnic group'),
# MAGIC ('L_ETH','Not Stated', 'Not Stated'),
# MAGIC ('L_ETH','Not Known', 'Not Known'),
# MAGIC ('L_ETH','UNKNOWN', 'UNKNOWN')

# COMMAND ----------

# DBTITLE 1,adding providers
# MAGIC %sql
# MAGIC 
# MAGIC INSERT INTO $db_output.asd_ref_master1
# MAGIC SELECT    DISTINCT    'Provider' as type
# MAGIC                     ,HDR.OrgIDProvider as level
# MAGIC                     ,ORG.NAME as level_description                    
# MAGIC FROM                $db_source.MHS000Header
# MAGIC                         AS HDR
# MAGIC LEFT OUTER JOIN (
# MAGIC                   SELECT DISTINCT ORG_CODE, 
# MAGIC                         NAME
# MAGIC                    FROM $db_output.asd_org_daily
# MAGIC                   WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
# MAGIC                         AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1)    
# MAGIC                         AND ORG_TYPE_CODE NOT IN ('MP', 'IR', 'F', 'GO', 'CN')                 
# MAGIC                 )
# MAGIC     AS ORG
# MAGIC     ON HDR.OrgIDProvider = ORG.ORG_CODE
# MAGIC INNER JOIN $db_output.Months as mth
# MAGIC    ON HDR.UniqMonthID = mth.UniqMonthID
# MAGIC INNER JOIN $db_output.Autism_12m_Referrals as ref
# MAGIC    ON HDR.OrgIDProvider = ref.OrgIDProv     --THIS JOIN LIMITS THE PROVIDER LIST TO THOSE WITH SOME AUTISM REFERRALS. THIS HAS BEEN LEFT OUT FOR NOW

# COMMAND ----------

# DBTITLE 1,add CCG/Sub-ICB list
# MAGIC %sql
# MAGIC INSERT INTO $db_output.asd_ref_master1
# MAGIC SELECT               'CCG or Sub ICB Location - GP Practice or Residence' as type
# MAGIC                      ,ORG_CODE as level
# MAGIC                            ,NAME as level_description
# MAGIC FROM                 $db_output.asd_org_daily
# MAGIC WHERE                (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL)
# MAGIC                      AND ORG_TYPE_CODE = 'CC'
# MAGIC                      AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ORG_CLOSE_DATE IS NULL)
# MAGIC                      AND ORG_OPEN_DATE <= '$rp_enddate'
# MAGIC                      AND NAME NOT LIKE '%HUB'
# MAGIC                      AND NAME NOT LIKE '%NATIONAL%'

# COMMAND ----------

# DBTITLE 1,add STP/ICB List
# MAGIC %sql
# MAGIC INSERT INTO $db_output.asd_ref_master1
# MAGIC SELECT DISTINCT      'STP or ICB - GP Practice or Residence' as type
# MAGIC                      ,STP_CODE as level
# MAGIC                      ,STP_NAME as level_description
# MAGIC FROM                 $db_output.asd_STP_Mapping

# COMMAND ----------

# DBTITLE 1,add Region List
# MAGIC %sql
# MAGIC INSERT INTO $db_output.asd_ref_master1
# MAGIC SELECT DISTINCT      'Commissioning Region - GP Practice or Residence' as type
# MAGIC                      ,REGION_CODE as level
# MAGIC                      ,REGION_NAME as level_description
# MAGIC FROM                 $db_output.asd_STP_Mapping

# COMMAND ----------

# DBTITLE 1,Insert Measures and Breakdowns into table
spark.sql(f"TRUNCATE TABLE {db_output}.measure_level_type_mapping")
metriclist = list(spark.sql(f"SELECT METRIC FROM {db_output}.asd_metric_name_lookup").select('METRIC').toPandas()['METRIC'])
for x in metriclist:
  spark.sql(f"""
INSERT INTO TABLE {db_output}.measure_level_type_mapping VALUES
  ('{x}', "England", 'England', Null, Null),
  ('{x}', "Age Group", 'AGE_LL', Null, Null),
  ('{x}', "Gender", 'GENDER', Null, Null),
  ('{x}', "Ethnicity", 'L_ETH', Null, Null),
  ('{x}',"CCG or Sub ICB Location - GP Practice or Residence",'CCG or Sub ICB Location - GP Practice or Residence',Null,Null),
  ('{x}',"CCG or Sub ICB Location - GP Practice or Residence; Age Group",'CCG or Sub ICB Location - GP Practice or Residence','AGE_LL',Null),
  ('{x}',"STP or ICB - GP Practice or Residence",'STP or ICB - GP Practice or Residence',Null,Null),
  ('{x}',"STP or ICB - GP Practice or Residence; Age Group",'STP or ICB - GP Practice or Residence','AGE_LL',Null),
  ('{x}',"Commissioning Region - GP Practice or Residence",'Commissioning Region - GP Practice or Residence',Null,Null),
  ('{x}',"Commissioning Region - GP Practice or Residence; Age Group",'Commissioning Region - GP Practice or Residence','AGE_LL',Null),
  ('{x}', "Provider", 'Provider', Null, Null),
  ('{x}',"Provider; Age Group",'Provider','AGE_LL',Null)""")

# COMMAND ----------

# DBTITLE 1,Creation of ref asset
'''From this cell onwards, temporary tables are created to create asd_csv_master. These are not built in schemas as they are only temp tables and they are dropped in cmd 19'''
spark.sql(f"OPTIMIZE {db_output}.asd_ref_master1 ZORDER BY (type)")
 
mltm_df = spark.table(f'{db_output}.measure_level_type_mapping')
ref_master_df = spark.table(f'{db_output}.asd_ref_master1')
joined_df = mltm_df.alias('lhs').join(ref_master_df.alias('rhs'), ref_master_df.type == mltm_df.type).select('lhs.metric','lhs.breakdown', 'lhs.type','lhs.cross_join_tables_l2','lhs.cross_join_tables_l3', 'rhs.level','rhs.level_description')
joined_df.createOrReplaceTempView("joined_df_v")
mltm_df.createOrReplaceTempView("mltm_df_v")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {db_output}.tmp_joined_df")
spark.sql(f"CREATE TABLE {db_output}.tmp_joined_df USING DELTA AS SELECT * FROM joined_df_v")

spark.sql(f"DROP TABLE IF EXISTS {db_output}.tmp_mltm_df_v")
spark.sql(f"CREATE TABLE {db_output}.tmp_mltm_df_v USING DELTA AS SELECT * FROM mltm_df_v")

spark.sql(f"DROP TABLE IF EXISTS {db_output}.level1_df_v")
spark.sql(f"CREATE TABLE {db_output}.level1_df_v USING DELTA AS SELECT * FROM {db_output}.tmp_joined_df WHERE cross_join_tables_l2 IS NULL AND cross_join_tables_l3 IS NULL")

spark.sql(f"DROP TABLE IF EXISTS {db_output}.level_2_stg1_df_v")
spark.sql(f"CREATE TABLE {db_output}.level_2_stg1_df_v USING DELTA AS SELECT * FROM {db_output}.tmp_mltm_df_v WHERE cross_join_tables_l2 IS NOT NULL AND cross_join_tables_l3 IS NULL")
          
spark.sql(f"TRUNCATE TABLE {db_output}.asd_csv_master")

# COMMAND ----------

qry_stg1 = f"""
INSERT INTO {db_output}.asd_csv_master
SELECT             
 m.ReportingPeriodStartDate as REPORTING_PERIOD_START
,m.ReportingPeriodEndDate as REPORTING_PERIOD_END
,'{status}' AS STATUS
,l.breakdown AS BREAKDOWN
,l.LEVEL AS LEVEL_ONE
,l.LEVEL_DESCRIPTION AS LEVEL_ONE_DESCRIPTION
,'NONE' AS LEVEL_TWO
,'NONE' AS LEVEL_TWO_DESCRIPTION
,l.metric AS METRIC
FROM {db_output}.level1_df_v as l
CROSS JOIN {db_output}.Months as m"""

print(qry_stg1)
spark.sql(qry_stg1)

# COMMAND ----------

level_2_stg1_df = spark.sql(f"select * from {db_output}.level_2_stg1_df_v")
print(level_2_stg1_df.count())
for record in level_2_stg1_df.collect():
  print(record)
  level2_qry = f"""
  INSERT INTO {db_output}.asd_csv_master
   SELECT             
     m.ReportingPeriodStartDate as REPORTING_PERIOD_START
    ,m.ReportingPeriodEndDate as REPORTING_PERIOD_END
    ,'{status}' AS STATUS
    ,l1.breakdown AS BREAKDOWN
    ,L1.level AS LEVEL_ONE
    ,L1.level_description AS LEVEL_ONE_DESCRIPTION
    ,L2.LEVEL AS LEVEL_TWO
    ,L2.LEVEL_DESCRIPTION AS LEVEL_TWO_DESCRIPTION
    ,L1.metric AS METRIC
    FROM 
    (SELECT * FROM {db_output}.tmp_joined_df WHERE type = '{record['type']}' AND metric = '{record['metric']}' AND breakdown = '{record['breakdown']}') AS L1
    CROSS JOIN (SELECT * FROM {db_output}.asd_ref_master1 where type = '{record['cross_join_tables_l2']}') AS L2
    CROSS JOIN {db_output}.Months as m
  """
  
  print(level2_qry)
  spark.sql(level2_qry)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE $db_output.asd_csv_master 

# COMMAND ----------

# MAGIC %sql
# MAGIC --CLEAN UP TEMPORARY TABLES
# MAGIC DROP TABLE IF EXISTS $db_output.tmp_joined_df;
# MAGIC DROP TABLE IF EXISTS $db_output.tmp_mltm_df_v;
# MAGIC DROP TABLE IF EXISTS $db_output.level1_df_v;
# MAGIC DROP TABLE IF EXISTS $db_output.level_2_stg1_df_v;