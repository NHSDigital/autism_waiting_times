# Databricks notebook source
# MAGIC %md
# MAGIC #Autism Statistics
# MAGIC ##Rolling 12 months
# MAGIC ###Create Common Tables
# MAGIC 
# MAGIC This notebook creates tables for the autism waiting times statistics quarterly publication.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Tables created in this notebook
# MAGIC 
# MAGIC - asd_metric_name_lookup
# MAGIC - asd_csv_master
# MAGIC - asd_ref_master1
# MAGIC - asd_org_daily
# MAGIC - asd_org_relationship_daily
# MAGIC - measure_level_type_mapping
# MAGIC - asd_output
# MAGIC - Autism_12m_Referrals
# MAGIC - Autism_12m_Referrals_ASD17
# MAGIC - autismdiagcodes
# MAGIC - Diagnoses
# MAGIC - carecontacts
# MAGIC - WaitTimes
# MAGIC - validcodes
# MAGIC - asd_dq_output_raw
# MAGIC - asd_dq_output_suppressed
# MAGIC - asd_main_output_raw
# MAGIC - asd_main_output_suppressed
# MAGIC - asd_test_log
# MAGIC 
# MAGIC ### Notes:
# MAGIC The below tables are created temporarily in notebooks/02_create_master_reference_data. They are not created here as they are dropped once finished with.
# MAGIC - tmp_joined_df
# MAGIC - tmp_mltm_df_v
# MAGIC - level1_df_v
# MAGIC - level_2_stg1_df_v

# COMMAND ----------

# spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Create Months table
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS $db_output.months;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.months
# MAGIC (
# MAGIC     ReportingPeriodStartDate date,
# MAGIC     ReportingPeriodEndDate date,
# MAGIC     UniqMonthID bigint,
# MAGIC     YEAR string,
# MAGIC     Month string,
# MAGIC     YearMonth string,
# MAGIC     OutputFile string
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Create Referral Months table
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS $db_output.referral_months;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.referral_months
# MAGIC (
# MAGIC     UniqMonthID bigint,
# MAGIC     UniqServReqID string,
# MAGIC     ReportingPeriodStartDate date,
# MAGIC     ReportingPeriodEndDate date
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC --Create lookup table for Metrics
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_metric_name_lookup;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_metric_name_lookup
# MAGIC (
# MAGIC     METRIC STRING,
# MAGIC     METRIC_DESCRIPTION STRING
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_csv_master;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_csv_master(
# MAGIC   reporting_period_start STRING,
# MAGIC   reporting_period_end STRING,
# MAGIC   status STRING,
# MAGIC   breakdown STRING,
# MAGIC   PRIMARY_LEVEL STRING,
# MAGIC   PRIMARY_LEVEL_DESCRIPTION STRING,
# MAGIC   SECONDARY_LEVEL STRING,
# MAGIC   SECONDARY_LEVEL_DESCRIPTION STRING,
# MAGIC   metric STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC  
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_ref_master1;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_ref_master1
# MAGIC (
# MAGIC     type STRING,
# MAGIC     level STRING,
# MAGIC     level_description STRING
# MAGIC )
# MAGIC using delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_org_daily;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_org_daily
# MAGIC (
# MAGIC     ORG_CODE STRING,
# MAGIC     NAME STRING,
# MAGIC     ORG_TYPE_CODE STRING,
# MAGIC     ORG_OPEN_DATE DATE,
# MAGIC     ORG_CLOSE_DATE DATE, 
# MAGIC     BUSINESS_START_DATE DATE,
# MAGIC     BUSINESS_END_DATE DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_org_relationship_daily;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_org_relationship_daily
# MAGIC (
# MAGIC     REL_TYPE_CODE STRING,
# MAGIC     REL_FROM_ORG_CODE STRING,
# MAGIC     REL_TO_ORG_CODE STRING,
# MAGIC     REL_OPEN_DATE DATE,
# MAGIC     REL_CLOSE_DATE DATE
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_STP_Mapping;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_STP_Mapping
# MAGIC (
# MAGIC     STP_CODE STRING,
# MAGIC     STP_NAME STRING,
# MAGIC     CCG_CODE STRING, 
# MAGIC     CCG_NAME STRING,
# MAGIC     REGION_CODE STRING,
# MAGIC     REGION_NAME STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_rd_ccg_latest;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_RD_CCG_LATEST
# MAGIC (
# MAGIC     ORG_TYPE_CODE string,
# MAGIC     ORG_CODE string,
# MAGIC     NAME string
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_ccg_prep;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_CCG_prep
# MAGIC (
# MAGIC     Person_ID string,
# MAGIC 	recordnumber decimal(23,0)
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_ccg_latest;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_CCG_LATEST
# MAGIC (
# MAGIC     Person_ID string,
# MAGIC     IC_Rec_CCG string,
# MAGIC     NAME string
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS $db_output.measure_level_type_mapping;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.measure_level_type_mapping
# MAGIC (
# MAGIC     metric STRING,
# MAGIC     breakdown STRING,
# MAGIC     type STRING, -- foreign key rel to ref_master.type
# MAGIC     cross_join_tables_l2 STRING, -- level1 cross join table
# MAGIC     cross_join_tables_l3 STRING -- level2 cross join table
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_output;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_output
# MAGIC  (REPORTING_PERIOD_START      DATE,
# MAGIC   REPORTING_PERIOD_END        DATE,
# MAGIC   STATUS                      STRING,
# MAGIC   BREAKDOWN                   STRING,
# MAGIC   PRIMARY_LEVEL               STRING,
# MAGIC   PRIMARY_LEVEL_DESCRIPTION   STRING,
# MAGIC   SECONDARY_LEVEL             STRING,
# MAGIC   SECONDARY_LEVEL_DESCRIPTION STRING,
# MAGIC   METRIC                      STRING,
# MAGIC   METRIC_VALUE                STRING,
# MAGIC   SOURCE_DB                   STRING)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS $db_output.autism_12m_referrals;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.Autism_12m_Referrals
# MAGIC  (Person_ID string,
# MAGIC   UniqServReqID string,
# MAGIC   UniqMonthID bigint,
# MAGIC   PrimReasonReferralMH string,
# MAGIC   RecordStartDate date,
# MAGIC   RecordEndDate date,
# MAGIC   ReferralRequestReceivedDate date,
# MAGIC   ServDischDate date,
# MAGIC   RecordNumber bigint,
# MAGIC   OrgIDProv string,
# MAGIC   CCGCode string,
# MAGIC   CCGName string,
# MAGIC   STPCode string,
# MAGIC   STPName string,
# MAGIC   RegionCode string,
# MAGIC   RegionName string,
# MAGIC   Age_Group string,
# MAGIC   Age_Group_Higher string,
# MAGIC   Der_Gender string,
# MAGIC   LowerEthnicity string,
# MAGIC   LowerEthnicity_Desc string,
# MAGIC   DER_GENDER_Prov string,
# MAGIC   LowerEthnicity_prov string,
# MAGIC   LowerEthnicity_Desc_prov string)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS $db_output.autism_12m_referrals_asd17;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.Autism_12m_Referrals_ASD17
# MAGIC  (Person_ID string,
# MAGIC   UniqServReqID string,
# MAGIC   UniqMonthID bigint,
# MAGIC   PrimReasonReferralMH string,
# MAGIC   RecordStartDate date,
# MAGIC   RecordEndDate date,
# MAGIC   ReferralRequestReceivedDate date,
# MAGIC   ServDischDate date,
# MAGIC   RecordNumber bigint,
# MAGIC   OrgIDProv string,
# MAGIC   CCGCode string,
# MAGIC   CCGName string,
# MAGIC   STPCode string,
# MAGIC   STPName string,
# MAGIC   RegionCode string,
# MAGIC   RegionName string,
# MAGIC   Age_Group string,
# MAGIC   Age_Group_Higher string,
# MAGIC   Der_Gender string,
# MAGIC   LowerEthnicity string,
# MAGIC   LowerEthnicity_Desc string,
# MAGIC   DER_GENDER_Prov string,
# MAGIC   LowerEthnicity_prov string,
# MAGIC   LowerEthnicity_Desc_prov string)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS $db_output.autismdiagcodes;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.autismdiagcodes
# MAGIC  (CODE string)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS $db_output.diagnoses ;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.Diagnoses 
# MAGIC  (ReportingPeriodStartDate date,
# MAGIC   ReportingPeriodEndDate date,
# MAGIC   UniqMonthID bigint,
# MAGIC   YEAR string,
# MAGIC   Month string,
# MAGIC   YearMonth string,
# MAGIC   OutputFile string,
# MAGIC   DiagDate string,
# MAGIC   Person_ID string,
# MAGIC   UniqServReqID string,
# MAGIC   Diag string)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS $db_output.carecontacts ;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.carecontacts 
# MAGIC  (Person_ID string,
# MAGIC   UniqServReqID string,
# MAGIC   ConsMechanismMH string,
# MAGIC   CareContDate date)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS $db_output.autism_diagnoses ;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.Autism_Diagnoses 
# MAGIC  (ReportingPeriodStartDate date,
# MAGIC   ReportingPeriodEndDate date,
# MAGIC   UniqMonthID bigint,
# MAGIC   YEAR string,
# MAGIC   Month string,
# MAGIC   YearMonth string,
# MAGIC   OutputFile string,
# MAGIC   UniqServReqID string,
# MAGIC   Person_ID string,
# MAGIC   ReferralRequestReceivedDate date,
# MAGIC   ServDischDate date,
# MAGIC   OrgIDProv string,
# MAGIC   Name string,
# MAGIC   CCGCode string,
# MAGIC   CCGName string,
# MAGIC   STPCode string,
# MAGIC   STPName string,
# MAGIC   RegionCode string,
# MAGIC   RegionName string,
# MAGIC   Age_Group string,
# MAGIC   Age_Group_Higher string,
# MAGIC   DER_GENDER string,
# MAGIC   LowerEthnicity string,
# MAGIC   LowerEthnicity_Desc string,
# MAGIC   DER_GENDER_Prov string,
# MAGIC   LowerEthnicity_Prov string,
# MAGIC   LowerEthnicity_Desc_Prov string,
# MAGIC   diag string)
# MAGIC USING delta
# MAGIC         

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS $db_output.waittimes ;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.WaitTimes 
# MAGIC  (UniqMonthID bigint,
# MAGIC   UniqServReqID string,
# MAGIC   ReportingPeriodStartDate date,
# MAGIC   ReportingPeriodEndDate date,
# MAGIC   Person_ID string,
# MAGIC   CareContDate date,
# MAGIC   ReferralRequestReceivedDate date,
# MAGIC   ServDischDate date,
# MAGIC   OrgIDProv string,
# MAGIC   Name string,
# MAGIC   CCGCode string,
# MAGIC   CCGName string,
# MAGIC   STPCode string,
# MAGIC   STPName string,
# MAGIC   RegionCode string,
# MAGIC   RegionName string,
# MAGIC   Age_Group string,
# MAGIC   Age_Group_Higher string,
# MAGIC   DER_GENDER string,
# MAGIC   LowerEthnicity string,
# MAGIC   LowerEthnicity_Desc string,
# MAGIC   DER_GENDER_Prov string,
# MAGIC   LowerEthnicity_Prov string,
# MAGIC   LowerEthnicity_Desc_Prov string,
# MAGIC   waittime int,
# MAGIC   WaitLength string,
# MAGIC   Wait13WeeksPlus string,
# MAGIC   Wait26WeeksPlus string,
# MAGIC   Wait12MonthsPlus string)
# MAGIC USING delta
# MAGIC         

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- DROP TABLE IF EXISTS $db_output.validcodes;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.validcodes
# MAGIC (
# MAGIC   Datatable string,
# MAGIC   Field string,
# MAGIC   Measure string,
# MAGIC   Type string,
# MAGIC   ValidValue string,
# MAGIC   FirstMonth int,
# MAGIC   LastMonth int
# MAGIC )
# MAGIC 
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (Datatable)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_output_raw;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_output_raw
# MAGIC  (REPORTING_PERIOD_START      DATE,
# MAGIC   REPORTING_PERIOD_END        DATE,
# MAGIC   STATUS                      STRING,
# MAGIC   BREAKDOWN                   STRING,
# MAGIC   PRIMARY_LEVEL               STRING,
# MAGIC   PRIMARY_LEVEL_DESCRIPTION   STRING,
# MAGIC   SECONDARY_LEVEL             STRING,
# MAGIC   SECONDARY_LEVEL_DESCRIPTION STRING,
# MAGIC   METRIC                      STRING,
# MAGIC   METRIC_NAME                 STRING,
# MAGIC   METRIC_VALUE                STRING,
# MAGIC   DB_SOURCE                   STRING,
# MAGIC   MAX_RP_STARTDATE            DATE)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_output_suppressed;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_output_suppressed
# MAGIC  (REPORTING_PERIOD_START      DATE,
# MAGIC   REPORTING_PERIOD_END        DATE,
# MAGIC   STATUS                      STRING,
# MAGIC   BREAKDOWN                   STRING,
# MAGIC   PRIMARY_LEVEL               STRING,
# MAGIC   PRIMARY_LEVEL_DESCRIPTION   STRING,
# MAGIC   SECONDARY_LEVEL             STRING,
# MAGIC   SECONDARY_LEVEL_DESCRIPTION STRING,
# MAGIC   METRIC                      STRING,
# MAGIC   METRIC_NAME                 STRING,
# MAGIC   METRIC_VALUE                STRING,
# MAGIC   DB_SOURCE                   STRING,
# MAGIC   MAX_RP_STARTDATE            DATE)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_main_output_raw;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_main_output_raw
# MAGIC  (REPORTING_PERIOD_START      DATE,
# MAGIC   REPORTING_PERIOD_END        DATE,
# MAGIC   STATUS                      STRING,
# MAGIC   BREAKDOWN                   STRING,
# MAGIC   PRIMARY_LEVEL               STRING,
# MAGIC   PRIMARY_LEVEL_DESCRIPTION   STRING,
# MAGIC   SECONDARY_LEVEL             STRING,
# MAGIC   SECONDARY_LEVEL_DESCRIPTION STRING,
# MAGIC   METRIC                      STRING,
# MAGIC   METRIC_NAME                 STRING,
# MAGIC   METRIC_VALUE                STRING)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_dq_output_raw;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_dq_output_raw
# MAGIC  (REPORTING_PERIOD_START      DATE,
# MAGIC   REPORTING_PERIOD_END        DATE,
# MAGIC   STATUS                      STRING,
# MAGIC   BREAKDOWN                   STRING,
# MAGIC   PRIMARY_LEVEL               STRING,
# MAGIC   PRIMARY_LEVEL_DESCRIPTION   STRING,
# MAGIC   SECONDARY_LEVEL             STRING,
# MAGIC   SECONDARY_LEVEL_DESCRIPTION STRING,
# MAGIC   METRIC                      STRING,
# MAGIC   METRIC_NAME                 STRING,
# MAGIC   METRIC_VALUE                STRING)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_main_output;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_main_output
# MAGIC  (REPORTING_PERIOD_START      DATE,
# MAGIC   REPORTING_PERIOD_END        DATE,
# MAGIC   STATUS                      STRING,
# MAGIC   BREAKDOWN                   STRING,
# MAGIC   PRIMARY_LEVEL               STRING,
# MAGIC   PRIMARY_LEVEL_DESCRIPTION   STRING,
# MAGIC   SECONDARY_LEVEL             STRING,
# MAGIC   SECONDARY_LEVEL_DESCRIPTION STRING,
# MAGIC   METRIC                      STRING,
# MAGIC   METRIC_NAME                 STRING,
# MAGIC   METRIC_VALUE                STRING)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_dq_output;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_dq_output
# MAGIC  (REPORTING_PERIOD_START      DATE,
# MAGIC   REPORTING_PERIOD_END        DATE,
# MAGIC   STATUS                      STRING,
# MAGIC   BREAKDOWN                   STRING,
# MAGIC   PRIMARY_LEVEL               STRING,
# MAGIC   PRIMARY_LEVEL_DESCRIPTION   STRING,
# MAGIC   SECONDARY_LEVEL             STRING,
# MAGIC   SECONDARY_LEVEL_DESCRIPTION STRING,
# MAGIC   METRIC                      STRING,
# MAGIC   METRIC_NAME                 STRING,
# MAGIC   METRIC_VALUE                STRING)
# MAGIC USING delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS $db_output.asd_test_log;
# MAGIC CREATE TABLE IF NOT EXISTS $db_output.asd_test_log
# MAGIC  (MAX_RP_STARTDATE            DATE,
# MAGIC   TABLENAME                   STRING,
# MAGIC   TEST                        STRING,
# MAGIC   RESULT                      STRING,
# MAGIC   METRIC                      STRING,
# MAGIC   BREAKDOWN                   STRING,
# MAGIC   MESSAGE                     STRING)
# MAGIC USING delta