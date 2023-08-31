# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # NB this contains all ValidCode lists for measures created in autism_waiting_times
# MAGIC ## if measures are removed from autism_waiting_times codebase they can be removed from here also  
# MAGIC 
# MAGIC ###
# MAGIC - in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
# MAGIC - subsequent changes in future years can use UPDATE statements
# MAGIC - please ensure table names are added into validcodes and used in join statements with the same Case.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC TRUNCATE TABLE $db_output.validcodes

# COMMAND ----------

# DBTITLE 1,INSERT mhs101referral codes INTO $db_output.validcodes
# MAGIC %sql
# MAGIC 
# MAGIC -- Table, Field, Measure, Type, ValidValue, FirstMonth, LastMonth
# MAGIC 
# MAGIC -- NB this contains all ValidCode lists for measures created in autism_waiting_times and should be kept in sync
# MAGIC 
# MAGIC -- in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
# MAGIC -- subsequent changes in future years can use UPDATE statements
# MAGIC 
# MAGIC INSERT INTO $db_output.validcodes
# MAGIC -- INSERT OVERWRITE TABLE $db_output.validcodes -- this will only work if all rows are included in a single statement - otherwise the later INSERTs OVERWRITE the initial one!
# MAGIC 
# MAGIC VALUES ('mhs101referral', 'PrimReasonReferralMH', 'ASD', 'include', '25', 1417, null)

# COMMAND ----------

# DBTITLE 1,INSERT mhs201carecontact codes INTO $db_output.validcodes
# MAGIC %sql
# MAGIC 
# MAGIC -- NB this contains all ValidCode lists for measures created in autism_waiting_times and should be kept in sync 
# MAGIC 
# MAGIC -- in order to keep the code from getting out of hand (lengthwise) this is a series of INSERT statements 
# MAGIC -- subsequent changes in future years can use UPDATE statements
# MAGIC 
# MAGIC INSERT INTO $db_output.validcodes
# MAGIC 
# MAGIC VALUES ('mhs201carecontact', 'ConsMechanismMH', 'ASD', 'include', '01', 1390, null)
# MAGIC ,('mhs201carecontact', 'ConsMechanismMH', 'ASD', 'include', '03', 1390, 1458)
# MAGIC ,('mhs201carecontact', 'ConsMechanismMH', 'ASD', 'include', '11', 1459, null)
# MAGIC 
# MAGIC ,('mhs201carecontact', 'AttendORDNACode', 'ASD', 'include', '5', 1390, null)
# MAGIC ,('mhs201carecontact', 'AttendORDNACode', 'ASD', 'include', '6', 1390, null)

# COMMAND ----------

# DBTITLE 1,ConsMechanismMH_dim
# %sql

# INSERT OVERWRITE TABLE $db_output.ConsMechanismMH_dim
# VALUES 
#   ('01', 'Face to face communication', 1429, 1458)
#   ,('01', 'Face to face', 1459, null)
#   ,('02', 'Telephone', 1429, null)
#   ,('03', 'Telemedicine web camera', 1429, 1458)
#   ,('04', 'Talk type for a person unable to speak', 1429, null)
#   ,('05', 'Email', 1429, null)
#   ,('06', 'Short Message Service (SMS) - Text Messaging', 1429, 1458)
#   ,('09', 'Text Message (Asynchronous)', 1459, null)
#   ,('10', 'Instant messaging (Synchronous)', 1459, null)
#   ,('11', 'Video consultation', 1459, null)
#   ,('12', 'Message Board (Asynchronous)', 1459, null)
#   ,('13', 'Chat Room (Synchronous)', 1459, null)
#   ,('98', 'Other', 1429, 1458)
#   ,('98', 'Other (not listed)', 1459, null)
#   ,('Invalid', 'Invalid', 1429, null)
#   ,('Missing', 'Missing', 1429, null)

# COMMAND ----------

# DBTITLE 1,referral_dim
# %sql

# INSERT OVERWRITE TABLE $db_output.referral_dim
# VALUES ('A', 'Primary Health Care', 1429, null)
# ,('B', 'Self Referral', 1429, null)
# ,('C', 'Local Authority Services', 1429, null)
# ,('D', 'Employer', 1429, null)
# ,('E', 'Justice System', 1429, null)
# ,('F', 'Child Health', 1429, null)
# ,('G', 'Independent/Voluntary Sector', 1429, null)
# ,('H', 'Acute Secondary Care', 1429, null)
# ,('I', 'Other Mental Health NHS Trust', 1429, null)
# ,('J', 'Internal referrals  from Community Mental Health Team (within own NHS Trust)', 1429, null)
# ,('K', 'Internal referrals from Inpatient Service (within own NHS Trust)', 1429, null)
# ,('L', 'Transfer by graduation (within own NHS Trust)', 1429, null)
# ,('M', 'Other', 1429, null)
# ,('N', 'Improving access to psychological  therapies', 1429, null)
# ,('P', 'Internal', 1429, null)
# ,('Q', 'Drop-in Services', 1459, null)
# ,('Invalid', 'Invalid', 1429, null)
# ,('Missing', 'Missing', 1429, null)