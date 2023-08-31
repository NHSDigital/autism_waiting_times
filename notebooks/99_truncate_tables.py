# Databricks notebook source
# MAGIC %md
# MAGIC #Autism Statistics
# MAGIC ##Rolling 12 months
# MAGIC ###Truncate Tables
# MAGIC 
# MAGIC This notebook truncates tables (excluding outputs) for the autism waiting times statistics quarterly publication.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Tables truncated in this notebook
# MAGIC 
# MAGIC - Autism_12m_Referrals
# MAGIC - Autism_12m_Referrals_ASD17
# MAGIC - Diagnoses
# MAGIC - carecontacts
# MAGIC - Autism_Diagnoses
# MAGIC - WaitTimes

# COMMAND ----------

# it's handy to have the parameters passed into this notebook here in case it needs running in isolation 

db_output = dbutils.widgets.get("db_output")

# COMMAND ----------

# spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE $db_output.Autism_12m_Referrals

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE $db_output.Autism_12m_Referrals_ASD17

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE $db_output.Diagnoses 

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE $db_output.carecontacts 

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE $db_output.Autism_Diagnoses 

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE $db_output.WaitTimes 

# COMMAND ----------

counts_metadata = {'ASD12' : {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN': 'ReferralRequestReceivedDate',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'ref_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD12a': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN': 'ReferralRequestReceivedDate',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'ref_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD12b': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN': 'ReferralRequestReceivedDate',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'ref_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD13' : {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN': 'ServDischDate',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'ref_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD13a': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN': 'ServDischDate',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'ref_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD13b': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN': 'ServDischDate',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'ref_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD16' : {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN': 'ReferralRequestReceivedDate',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': 'open_pat_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD16a': {'SOURCE_TABLE'  : 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN'    : 'ReferralRequestReceivedDate',
                              'COUNTCOLUMN'   : 'Person_ID',
                              'METRIC_TYPE'   : 'open_13_pat_count',
                              'FILTERCOLUMN'  : '',
                              'FILTERVALUE'   : ''},
                   'ASD16b': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN': 'ReferralRequestReceivedDate',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': 'open_pat_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD16c': {'SOURCE_TABLE'  : 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN'    : 'ReferralRequestReceivedDate',
                              'COUNTCOLUMN'   : 'Person_ID',
                              'METRIC_TYPE'   : 'open_13_pat_count',
                              'FILTERCOLUMN'  : '',
                              'FILTERVALUE'   : ''},
                   'ASD16d': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN': 'ReferralRequestReceivedDate',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': 'open_pat_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD16e': {'SOURCE_TABLE'  : 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN'    : 'ReferralRequestReceivedDate',
                              'COUNTCOLUMN'   : 'Person_ID',
                              'METRIC_TYPE'   : 'open_13_pat_count',
                              'FILTERCOLUMN'  : '',
                              'FILTERVALUE'   : ''},
                   'ASD20' : {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN': 'CareContDate',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': 'contacts_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD20a': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN': 'CareContDate',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': 'contacts_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD20b': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN': 'CareContDate',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': 'contacts_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD21' : {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN': 'CareContDate',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': 'ref_diag_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD21a': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN': 'CareContDate',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': 'ref_diag_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD21b': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN': 'CareContDate',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': 'ref_diag_count',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD23' : {'SOURCE_TABLE': 'Autism_Diagnoses',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN': 'ServDischDate',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'diag_count',
                              'FILTERCOLUMN': 'diag',
                              'FILTERVALUE': '"Other F"'},
                   'ASD23a': {'SOURCE_TABLE': 'Autism_Diagnoses',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN': 'ServDischDate',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'diag_count',
                              'FILTERCOLUMN': 'diag',
                              'FILTERVALUE': '"Other F"'},
                   'ASD23b': {'SOURCE_TABLE': 'Autism_Diagnoses',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN': 'ServDischDate',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'diag_count',
                              'FILTERCOLUMN': 'diag',
                              'FILTERVALUE': '"Other F"'},
                   'ASD24' : {'SOURCE_TABLE': 'Autism_Diagnoses',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN': 'ServDischDate',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'diag_count',
                              'FILTERCOLUMN': 'diag',
                              'FILTERVALUE': '"Other"'},
                   'ASD24a': {'SOURCE_TABLE': 'Autism_Diagnoses',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN': 'ServDischDate',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'diag_count',
                              'FILTERCOLUMN': 'diag',
                              'FILTERVALUE': '"Other"'},
                   'ASD24b': {'SOURCE_TABLE': 'Autism_Diagnoses',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN': 'ServDischDate',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'diag_count',
                              'FILTERCOLUMN': 'diag',
                              'FILTERVALUE': '"Other"'},
                   'ASD18' : {'SOURCE_TABLE': 'WaitTimes',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': '',
                              'FILTERCOLUMN': 'WaitLength',
                              'FILTERVALUE': '"Less than 13 weeks"'},
                   'ASD18a': {'SOURCE_TABLE'  : 'WaitTimes',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN'    : '',
                              'COUNTCOLUMN'   : 'Person_ID',
                              'METRIC_TYPE'   : '',
                              'FILTERCOLUMN'  : 'WaitLength',
                              'FILTERVALUE'   : '"Less than 13 weeks","More than 13 weeks"'},
                   'ASD18b': {'SOURCE_TABLE': 'WaitTimes',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': '',
                              'FILTERCOLUMN': 'WaitLength',
                              'FILTERVALUE': '"No Contact"'},
                   'ASD18c': {'SOURCE_TABLE': 'WaitTimes',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': '',
                              'FILTERCOLUMN': 'WaitLength',
                              'FILTERVALUE': '"Less than 13 weeks"'},
                   'ASD18d': {'SOURCE_TABLE': 'WaitTimes',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': '',
                              'FILTERCOLUMN': 'WaitLength',
                              'FILTERVALUE': '"Less than 13 weeks"'},
                   'ASD18e': {'SOURCE_TABLE'  : 'WaitTimes',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN'    : '',
                              'COUNTCOLUMN'   : 'Person_ID',
                              'METRIC_TYPE'   : '',
                              'FILTERCOLUMN'  : 'WaitLength',
                              'FILTERVALUE'   : '"Less than 13 weeks","More than 13 weeks"'},
                   'ASD18f': {'SOURCE_TABLE': 'WaitTimes',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': '',
                              'FILTERCOLUMN': 'WaitLength',
                              'FILTERVALUE': '"No Contact"'},
                   'ASD18g': {'SOURCE_TABLE'  : 'WaitTimes',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN'    : '',
                              'COUNTCOLUMN'   : 'Person_ID',
                              'METRIC_TYPE'   : '',
                              'FILTERCOLUMN'  : 'WaitLength',
                              'FILTERVALUE'   : '"Less than 13 weeks","More than 13 weeks"'},
                   'ASD18h': {'SOURCE_TABLE': 'WaitTimes',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': '',
                              'FILTERCOLUMN': 'WaitLength',
                              'FILTERVALUE': '"No Contact"'},
                   'ASD11' : {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'open_refs',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD11a': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'open_refs',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD11b': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'open_refs',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD14' : {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'last_refs',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD14a': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'last_refs',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD14b': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'UniqServReqID',
                              'METRIC_TYPE': 'last_refs',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD17' : {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': '',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD17a': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': 'Under 18',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': '',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''},
                   'ASD17b': {'SOURCE_TABLE': 'Autism_12m_Referrals',
                              'AGEGROUPHIGHER': '18 and over',
                              'DATECOLUMN': '',
                              'COUNTCOLUMN': 'Person_ID',
                              'METRIC_TYPE': '',
                              'FILTERCOLUMN': '',
                              'FILTERVALUE': ''}
                  }

# COMMAND ----------

'''Tidy up source data tables'''

for metric in counts_metadata:
  spark.sql(f"""DROP TABLE IF EXISTS {db_output}.data_{metric}""")
  spark.sql(f"""DROP TABLE IF EXISTS {db_output}.data_{metric}_Prov""")