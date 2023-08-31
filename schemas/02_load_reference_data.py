# Databricks notebook source
'''This notebook creates the list of metric names and autism diagnosis codes'''

# COMMAND ----------

# DBTITLE 1,Create Metric Name Lookup
# MAGIC %sql
# MAGIC 
# MAGIC TRUNCATE TABLE $db_output.asd_metric_name_lookup;
# MAGIC INSERT INTO $db_output.asd_metric_name_lookup VALUES
# MAGIC ('ASD12','Number of new suspected autism referrals in the month'),
# MAGIC ('ASD12a','Number of new suspected autism referrals for those aged 0 to 17 in the month'),
# MAGIC ('ASD12b','Number of new suspected autism referrals for those aged 18 and over in the month'),
# MAGIC ('ASD13','Number of closed suspected autism referrals in the month'),
# MAGIC ('ASD13a','Number of closed suspected autism referrals for those aged 0 to 17 in the month'),
# MAGIC ('ASD13b','Number of closed suspected autism referrals for those aged 18 and over in the month'),
# MAGIC ('ASD16','Number of patients with an open suspected autism referral in the month'),
# MAGIC ('ASD16a','Number of patients with an open suspected autism referral in the month that has been open for at least 13 weeks'),
# MAGIC ('ASD16b','Number of patients aged 0 to 17 with an open suspected autism referral in the month'),
# MAGIC ('ASD16c','Number of patients aged 0 to 17 with an open suspected autism referral in the month that has been open for at least 13 weeks'),
# MAGIC ('ASD16d','Number of patients aged 18 and over with an open suspected autism referral in the month'),
# MAGIC ('ASD16e','Number of patients aged 18 and over with an open suspected autism referral in the month that has been open for at least 13 weeks'),
# MAGIC ('ASD20','Number of patients which have an open suspected autism referral in the month receiving at least one care contact in the month'),
# MAGIC ('ASD20a','Number of patients aged 0 to 17 which have an open suspected autism referral in the month receiving at least one care contact in the month'),
# MAGIC ('ASD20b','Number of patients aged 18 and over which have an open suspected autism referral in the month receiving at least one care contact in the month'),
# MAGIC ('ASD21','Number of patients with an open suspected autism referral receiving an autism diagnosis in the month'),
# MAGIC ('ASD21a','Number of patients aged 0 to 17 with an open suspected autism referral receiving an autism diagnosis in the month'),
# MAGIC ('ASD21b','Number of patients aged 18 and over with an open suspected autism referral receiving an autism diagnosis in the month'),
# MAGIC ('ASD23','Number of patients with an open suspected autism referral receiving a Mental and Behavioural disorder diagnosis in the month that is not an autism diagnosis'),
# MAGIC ('ASD23a','Number of patients aged 0 to 17 with an open suspected autism referral receiving a Mental and Behavioural disorder diagnosis in the month that is not an autism diagnosis'),
# MAGIC ('ASD23b','Number of patients aged 18 and over with an open suspected autism referral receiving a Mental and Behavioural disorder diagnosis in the month that is not an autism diagnosis'),
# MAGIC ('ASD24','Number of patients with an open suspected autism referral receiving a non-Mental and Behavioural disorder diagnosis recorded in the month'),
# MAGIC ('ASD24a','Number of patients aged 0 to 17 with an open suspected autism referral receiving a non-Mental and Behavioural disorder diagnosis recorded in the month'),
# MAGIC ('ASD24b','Number of patients aged 18 and over with an open suspected autism referral receiving a non-Mental and Behavioural disorder diagnosis recorded in the month'),
# MAGIC --Waiting Time Metrics
# MAGIC ('ASD18','Number of patients with an open suspected autism referral in the month that has been open for at least 13 weeks receiving a first appointment in 13 weeks or less'),
# MAGIC ('ASD18a','Number of patients with an open suspected autism referral in the month that has been open for at least 13 weeks receiving a first appointment after more than 13 weeks'),
# MAGIC ('ASD18b','Number of patients with an open suspected autism referral in the month that has been open for at least 13 weeks that have not had a care contact appointment recorded'),
# MAGIC ('ASD18c','Number of patients aged 0 to 17 with an open suspected autism referral in the month that has been open for at least 13 weeks receiving a first appointment in 13 weeks or less'),
# MAGIC ('ASD18d','Number of patients aged 18 and over with an open suspected autism referral in the month that has been open for at least 13 weeks receiving a first appointment in 13 weeks or less'),
# MAGIC ('ASD18e','Number of patients aged 0 to 17 with an open suspected autism referral in the month that has been open for at least 13 weeks receiving a first appointment after more than 13 weeks'),
# MAGIC ('ASD18f','Number of patients aged 0 to 17 with an open suspected autism referral in the month that has been open for at least 13 weeks that have not had a care contact appointment recorded'),
# MAGIC ('ASD18g','Number of patients aged 18 and over with an open suspected autism referral in the month that has been open for at least 13 weeks receiving a first appointment after more than 13 weeks'),
# MAGIC ('ASD18h','Number of patients aged 18 and over with an open suspected autism referral in the month that has been open for at least 13 weeks that have not had a care contact appointment recorded'),
# MAGIC ('ASD19','Proportion of patients with an open suspected autism referral in the month that has been open for at least 13 weeks receiving a first appointment in 13 weeks or less'),
# MAGIC ('ASD19a','Proportion of patients with an open suspected autism referral in the month that has been open for at least 13 weeks receiving a first appointment after more than 13 weeks'),
# MAGIC ('ASD19b','Proportion of patients with an open suspected autism referral in the month that has been open for at least 13 weeks that have not had a care contact appointment recorded'),
# MAGIC ('ASD19c','Proportion of patients aged 0 to 17 with an open suspected autism referral in the month that has been open for at least 13 weeks receiving a first appointment in 13 weeks or less'),
# MAGIC ('ASD19d','Proportion of patients aged 18 and over with an open suspected autism referral in the month that has been open for at least 13 weeks receiving a first appointment in 13 weeks or less'),
# MAGIC ('ASD19e','Proportion of patients aged 0 to 17 with an open suspected autism referral in the month that has been open for at least 13 weeks receiving a first appointment after more than 13 weeks'),
# MAGIC ('ASD19f','Proportion of patients aged 0 to 17 with an open suspected autism referral in the month that has been open for at least 13 weeks that have not had a care contact appointment recorded'),
# MAGIC ('ASD19g','Proportion of patients aged 18 and over with an open suspected autism referral in the month that has been open for at least 13 weeks receiving a first appointment after more than 13 weeks'),
# MAGIC ('ASD19h','Proportion of patients aged 18 and over with an open suspected autism referral in the month that has been open for at least 13 weeks that have not had a care contact appointment recorded'),
# MAGIC --DQ Metrics
# MAGIC ('ASD11','Number of open suspected autism referrals in the month'),
# MAGIC ('ASD11a','Number of open suspected autism referrals for those aged 0 to 17 in the month'),
# MAGIC ('ASD11b','Number of open suspected autism referrals for those aged 18 and over in the month'),
# MAGIC ('ASD14','Number of open, suspected autism referrals in the month that do not appear in any subsequent month whether discharged or not'),
# MAGIC ('ASD14a','Number of open, suspected autism referrals for those aged 0 to 17 in the month that do not appear in any subsequent month whether discharged or not'),
# MAGIC ('ASD14b','Number of open, suspected autism referrals for those aged 18 and over in the month that do not appear in any subsequent month whether discharged or not'),
# MAGIC ('ASD17','Number of patients with more than one open suspected autism referral in the month'),
# MAGIC ('ASD17a','Number of patients aged 0 to 17 with more than one open suspected autism referral in the month'),
# MAGIC ('ASD17b','Number of patients aged 18 and over with more than one open suspected autism referral in the month');

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE $db_output.autismdiagcodes;
# MAGIC 
# MAGIC INSERT INTO $db_output.autismdiagcodes
# MAGIC SELECT CODE
# MAGIC FROM $dss_corporate.icd10_codes
# MAGIC WHERE (CODE like 'F84%' --ICD10
# MAGIC          or CODE like 'F88%' --ICD10
# MAGIC          or CODE like 'F89%' --ICD10
# MAGIC                           )
# MAGIC     and CODE not like 'F842%'
# MAGIC     and CODE not like 'F84.2%'
# MAGIC     and CODE not like 'F844%'
# MAGIC     and CODE not like 'F84.4%'
# MAGIC UNION ALL
# MAGIC SELECT ALT_CODE
# MAGIC FROM $dss_corporate.icd10_codes
# MAGIC WHERE (CODE like 'F84%' --ICD10
# MAGIC          or CODE like 'F88%' --ICD10
# MAGIC          or CODE like 'F89%' --ICD10
# MAGIC                           )
# MAGIC     and CODE not like 'F842%'
# MAGIC     and CODE not like 'F84.2%'
# MAGIC     and CODE not like 'F844%'
# MAGIC     and CODE not like 'F84.4%'
# MAGIC UNION ALL
# MAGIC SELECT DISTINCT con.id
# MAGIC FROM $dss_corporate.snomed_ct_rf2_concepts as con
# MAGIC INNER JOIN $dss_corporate.snomed_ct_rf2_descriptions as des
# MAGIC    ON con.id = des.CONCEPT_ID
# MAGIC   AND con.MODULE_ID = des.MODULE_ID
# MAGIC WHERE con.id in ('408857007',
# MAGIC                   '43614003',
# MAGIC                   '191689008',
# MAGIC                   '191690004',
# MAGIC                   '231536004',
# MAGIC                   '23560001',
# MAGIC                   '702732007',
# MAGIC                   '708037001',
# MAGIC                   '408856003',
# MAGIC                   '373618009',
# MAGIC                   '442314000',
# MAGIC                   '35919005',
# MAGIC                   '39951000119105',
# MAGIC                   '870260008',
# MAGIC                   '870261007',
# MAGIC                   '870262000',
# MAGIC                   '870263005',
# MAGIC                   '870264004',
# MAGIC                   '870265003',
# MAGIC                   '870266002',
# MAGIC                   '870267006',
# MAGIC                   '870268001',
# MAGIC                   '870269009',
# MAGIC                   '870270005',
# MAGIC                   '870280009',
# MAGIC                   '870282001',
# MAGIC                   '870303005',
# MAGIC                   '870304004',
# MAGIC                   '870305003',
# MAGIC                   '870306002',
# MAGIC                   '870307006',
# MAGIC                   '870308001',
# MAGIC                   '722287002',
# MAGIC                   '723332005',
# MAGIC                   '733623005',
# MAGIC                   '766824003',
# MAGIC                   '770790004',
# MAGIC                   '771448004',
# MAGIC                   '771512003',
# MAGIC                   '783089006',
# MAGIC                   '432091002'
# MAGIC   )