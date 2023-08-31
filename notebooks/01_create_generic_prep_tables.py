# Databricks notebook source
# MAGIC %md
# MAGIC #Autism Statistics
# MAGIC ##Rolling 12 months
# MAGIC ###Generic Preparation Notebook Notebook
# MAGIC 
# MAGIC This notebook produces the generic preparation tables for the new autism statistics measures.

# COMMAND ----------

# dbutils.widgets.removeAll()

# COMMAND ----------

# dbutils.widgets.text("db_output", "")
# dbutils.widgets.text("db_source", "")
# dbutils.widgets.text("month_id", "")
# dbutils.widgets.text("rp_enddate","")
# dbutils.widgets.text("rp_startdate","")

# COMMAND ----------

db_output = dbutils.widgets.get("db_output")
db_source = dbutils.widgets.get("db_source")
month_id = dbutils.widgets.get("month_id")
rp_enddate = dbutils.widgets.get("rp_enddate")
rp_startdate = dbutils.widgets.get("rp_startdate")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE TABLE $db_output.Months
# MAGIC 
# MAGIC SELECT DISTINCT ReportingPeriodStartDate,
# MAGIC                 ReportingPeriodEndDate,
# MAGIC                 UniqMonthID,
# MAGIC                 CAST(YEAR(ReportingPeriodStartDate) as string) as YEAR,
# MAGIC                 RIGHT(CONCAT("0",CAST(MONTH(ReportingPeriodStartDate) as string)),2) as Month,
# MAGIC                 CONCAT(CAST(YEAR(ReportingPeriodStartDate) as string),
# MAGIC                         RIGHT(CONCAT("0",CAST(MONTH(ReportingPeriodStartDate) as string)),2)) as YearMonth,
# MAGIC                 CASE WHEN MONTH(ReportingPeriodStartDate) between 4 and 12 THEN CONCAT(CAST(YEAR(ReportingPeriodStartDate) as string),RIGHT(CAST(YEAR(ReportingPeriodStartDate)+1 as string),2))
# MAGIC                      ELSE CONCAT(CAST(YEAR(ReportingPeriodStartDate)-1 as string),RIGHT(CAST(YEAR(ReportingPeriodStartDate) as string),2)) END as OutputFile
# MAGIC FROM $db_source.MHS000Header
# MAGIC WHERE ReportingPeriodStartDate between '$rp_startdate' and '$rp_enddate'
# MAGIC ORDER BY UniqMonthID;
# MAGIC 
# MAGIC -- SELECT * FROM $db.Months;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- TRUNCATE TABLE $db_output.asd_org_daily;
# MAGIC INSERT OVERWRITE TABLE $db_output.asd_org_daily
# MAGIC SELECT DISTINCT ORG_CODE,
# MAGIC                 NAME,
# MAGIC                 ORG_TYPE_CODE,
# MAGIC                 ORG_OPEN_DATE, 
# MAGIC                 ORG_CLOSE_DATE, 
# MAGIC                 BUSINESS_START_DATE, 
# MAGIC                 BUSINESS_END_DATE
# MAGIC            FROM $dss_corporate.org_daily
# MAGIC           WHERE (BUSINESS_END_DATE >= add_months('$rp_enddate', 1) OR ISNULL(BUSINESS_END_DATE))
# MAGIC                 AND BUSINESS_START_DATE <= add_months('$rp_enddate', 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- TRUNCATE TABLE $db_output.asd_org_relationship_daily;
# MAGIC INSERT OVERWRITE TABLE $db_output.asd_org_relationship_daily
# MAGIC SELECT REL_TYPE_CODE,
# MAGIC        REL_FROM_ORG_CODE,
# MAGIC        REL_TO_ORG_CODE, 
# MAGIC        REL_OPEN_DATE,
# MAGIC        REL_CLOSE_DATE
# MAGIC FROM $dss_corporate.ORG_RELATIONSHIP_DAILY
# MAGIC WHERE (REL_CLOSE_DATE >= '$rp_enddate' OR ISNULL(REL_CLOSE_DATE))              
# MAGIC   AND REL_OPEN_DATE <= '$rp_enddate';

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- TRUNCATE TABLE $db_output.asd_RD_CCG_LATEST;
# MAGIC INSERT OVERWRITE TABLE $db_output.asd_RD_CCG_LATEST
# MAGIC SELECT DISTINCT ORG_TYPE_CODE, ORG_CODE, NAME
# MAGIC FROM $dss_corporate.ORG_DAILY
# MAGIC WHERE (BUSINESS_END_DATE >= '$rp_enddate' OR BUSINESS_END_DATE IS NULL)
# MAGIC   AND BUSINESS_START_DATE <= '$rp_enddate'
# MAGIC     AND ORG_TYPE_CODE = "CC"
# MAGIC       AND (ORG_CLOSE_DATE >= '$rp_enddate' OR ORG_CLOSE_DATE IS NULL)
# MAGIC         AND ORG_OPEN_DATE <= '$rp_enddate'
# MAGIC           AND NAME NOT LIKE '%HUB'
# MAGIC             AND NAME NOT LIKE '%NATIONAL%'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- TRUNCATE TABLE $db_output.asd_CCG_prep;
# MAGIC INSERT OVERWRITE TABLE $db_output.asd_CCG_prep
# MAGIC SELECT DISTINCT    a.Person_ID,
# MAGIC 				   max(a.RecordNumber) as recordnumber                
# MAGIC FROM               $db_source.MHS001MPI a
# MAGIC LEFT JOIN          $db_source.MHS002GP b 
# MAGIC 		           on a.Person_ID = b.Person_ID 
# MAGIC                    and a.UniqMonthID = b.UniqMonthID
# MAGIC 		           and a.recordnumber = b.recordnumber
# MAGIC 		           and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
# MAGIC 		           and b.EndDateGMPRegistration is null                
# MAGIC LEFT JOIN          $db_output.asd_RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
# MAGIC LEFT JOIN          $db_output.asd_RD_CCG_LATEST d on a.OrgIDSubICBLocResidence = d.ORG_CODE
# MAGIC LEFT JOIN          $db_output.asd_RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
# MAGIC LEFT JOIN          $db_output.asd_RD_CCG_LATEST f on b.OrgIDSubICBLocGP = f.ORG_CODE
# MAGIC WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null
# MAGIC                     or d.ORG_CODE is not null or f.ORG_CODE is not null)
# MAGIC                    and a.uniqmonthid between ('$month_id'-11) and '$month_id'        
# MAGIC GROUP BY           a.Person_ID;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- TRUNCATE TABLE $db_output.asd_CCG_LATEST;
# MAGIC INSERT OVERWRITE TABLE $db_output.asd_CCG_LATEST
# MAGIC select distinct    a.Person_ID,
# MAGIC 				   CASE 
# MAGIC                         WHEN a.UniqMonthID <= 1467 and b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN b.OrgIDCCGGPPractice
# MAGIC                         WHEN a.UniqMonthID > 1467 and b.OrgIDSubICBLocGP IS NOT NULL and g.ORG_CODE is not null THEN b.OrgIDSubICBLocGP
# MAGIC                         WHEN a.UniqMonthID <= 1467 and a.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN A.OrgIDCCGRes
# MAGIC 					    WHEN a.UniqMonthID > 1467 and a.OrgIDSubICBLocResidence IS NOT NULL and f.ORG_CODE is not null THEN a.OrgIDSubICBLocResidence
# MAGIC 						ELSE 'UNKNOWN' END AS IC_Rec_CCG, 
# MAGIC                    CASE 
# MAGIC                         WHEN a.UniqMonthID <= 1467 and b.OrgIDCCGGPPractice IS NOT NULL and e.ORG_CODE is not null THEN e.NAME
# MAGIC                         WHEN a.UniqMonthID > 1467 and b.OrgIDSubICBLocGP IS NOT NULL and g.ORG_CODE is not null THEN g.NAME
# MAGIC                         WHEN a.UniqMonthID <= 1467 and a.OrgIDCCGRes IS NOT NULL and c.ORG_CODE is not null THEN c.NAME
# MAGIC 					    WHEN a.UniqMonthID > 1467 and a.OrgIDSubICBLocResidence IS NOT NULL and f.ORG_CODE is not null THEN f.Name
# MAGIC 						ELSE 'UNKNOWN' END AS NAME-- Updated for Sub-ICB for data for December pub		
# MAGIC FROM               $db_source.mhs001MPI a
# MAGIC LEFT JOIN          $db_source.MHS002GP b 
# MAGIC                    on a.Person_ID = b.Person_ID 
# MAGIC                    and a.UniqMonthID = b.UniqMonthID  
# MAGIC                    and a.recordnumber = b.recordnumber
# MAGIC                    and b.GMPCodeReg NOT IN ('V81999','V81998','V81997')
# MAGIC                    and b.EndDateGMPRegistration is null
# MAGIC INNER JOIN         $db_output.asd_CCG_prep ccg on a.recordnumber = ccg.recordnumber
# MAGIC LEFT JOIN          $db_output.asd_RD_CCG_LATEST c on a.OrgIDCCGRes = c.ORG_CODE
# MAGIC LEFT JOIN          $db_output.asd_RD_CCG_LATEST f on a.OrgIDSubICBLocResidence  = f.ORG_CODE
# MAGIC LEFT JOIN          $db_output.asd_RD_CCG_LATEST e on b.OrgIDCCGGPPractice = e.ORG_CODE
# MAGIC LEFT JOIN          $db_output.asd_RD_CCG_LATEST g on b.OrgIDSubICBLocGP  = g.ORG_CODE
# MAGIC WHERE              (e.ORG_CODE is not null or c.ORG_CODE is not null or f.ORG_CODE is not null or g.ORG_CODE is not null)
# MAGIC                    and a.uniqmonthid between ('$month_id'-11) and '$month_id';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRUNCATE TABLE $db_output.asd_STP_Mapping;
# MAGIC INSERT OVERWRITE TABLE $db_output.asd_STP_Mapping
# MAGIC SELECT 
# MAGIC A.ORG_CODE as STP_CODE, 
# MAGIC A.NAME as STP_NAME, 
# MAGIC C.ORG_CODE as CCG_CODE, 
# MAGIC C.NAME as CCG_NAME,
# MAGIC E.ORG_CODE as REGION_CODE,
# MAGIC E.NAME as REGION_NAME
# MAGIC FROM 
# MAGIC $db_output.asd_org_daily A
# MAGIC LEFT JOIN $db_output.asd_org_relationship_daily B ON A.ORG_CODE = B.REL_TO_ORG_CODE AND B.REL_TYPE_CODE = 'CCST'
# MAGIC LEFT JOIN $db_output.asd_org_daily C ON B.REL_FROM_ORG_CODE = C.ORG_CODE
# MAGIC LEFT JOIN $db_output.asd_org_relationship_daily D ON A.ORG_CODE = D.REL_FROM_ORG_CODE AND D.REL_TYPE_CODE = 'STCE'
# MAGIC LEFT JOIN $db_output.asd_org_daily E ON D.REL_TO_ORG_CODE = E.ORG_CODE
# MAGIC WHERE
# MAGIC A.ORG_TYPE_CODE = 'ST'
# MAGIC AND B.REL_TYPE_CODE is not null
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRUNCATE TABLE $db_output.referral_months;
# MAGIC INSERT OVERWRITE TABLE $db_output.referral_months
# MAGIC 
# MAGIC SELECT DISTINCT ref.UniqMonthID,
# MAGIC                 ref.UniqServReqID,
# MAGIC                 mth.ReportingPeriodStartDate,
# MAGIC                 mth.ReportingPeriodEndDate
# MAGIC FROM $db_source.MHS101Referral as ref
# MAGIC INNER JOIN $db_output.Months as mth
# MAGIC    ON ref.UniqMonthID = mth.UniqMonthID
# MAGIC WHERE ReferralRequestReceivedDate >= '2018-04-01'
# MAGIC   AND PrimReasonReferralMH = '25'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRUNCATE TABLE $db_output.Autism_12m_Referrals;
# MAGIC INSERT OVERWRITE TABLE $db_output.Autism_12m_Referrals
# MAGIC 
# MAGIC SELECT  ref.Person_ID,
# MAGIC         ref.UniqServReqID,
# MAGIC         ref.UniqMonthID,
# MAGIC         ref.PrimReasonReferralMH,
# MAGIC         ref.RecordStartDate,
# MAGIC         ref.RecordEndDate,
# MAGIC         ref.ReferralRequestReceivedDate,
# MAGIC         ref.ServDischDate,
# MAGIC         ref.RecordNumber,
# MAGIC         ref.OrgIDProv,
# MAGIC         ccg.IC_Rec_CCG AS CCGCode,
# MAGIC         ccg.NAME AS CCGName,
# MAGIC         stp.STP_CODE AS STPCode,
# MAGIC         stp.STP_NAME AS STPName,
# MAGIC         stp.REGION_CODE AS RegionCode,
# MAGIC         stp.REGION_NAME AS RegionName,
# MAGIC       --THIS SECTION ADDS NATIONAL LEVEL DEMOGRAPHICS
# MAGIC         (CASE
# MAGIC             WHEN ref.AgeServReferRecDate between 0 and 9
# MAGIC             THEN 'Under 10'
# MAGIC             WHEN ref.AgeServReferRecDate between 10 and 17
# MAGIC             THEN '10 to 17'
# MAGIC             WHEN ref.AgeServReferRecDate between 18 and 24
# MAGIC             THEN '18 to 24'
# MAGIC             WHEN ref.AgeServReferRecDate between 25 and 34
# MAGIC             THEN '25 to 34'
# MAGIC             WHEN ref.AgeServReferRecDate between 35 and 44
# MAGIC             THEN '35 to 44'
# MAGIC             WHEN ref.AgeServReferRecDate between 45 and 54
# MAGIC             THEN '45 to 54'
# MAGIC             WHEN ref.AgeServReferRecDate between 55 and 64
# MAGIC             THEN '55 to 64'
# MAGIC             WHEN ref.AgeServReferRecDate > 64
# MAGIC             THEN '65 and Over'
# MAGIC             ELSE 'UNKNOWN'
# MAGIC         END) AS Age_Group,
# MAGIC         (CASE
# MAGIC             WHEN ref.AgeServReferRecDate is NULL
# MAGIC             THEN 'UNKNOWN'
# MAGIC             WHEN ref.AgeServReferRecDate between 0 and 17
# MAGIC             THEN 'Under 18'
# MAGIC             ELSE '18 and over'
# MAGIC         END) AS Age_Group_Higher,
# MAGIC         CASE WHEN mpi.GenderIDCode IN ('1','2','3','4') THEN mpi.GenderIDCode
# MAGIC             WHEN mpi.Gender IN ('1','2','9') THEN mpi.Gender
# MAGIC             ELSE 'UNKNOWN' END AS Der_Gender,
# MAGIC         CASE WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
# MAGIC              WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
# MAGIC              WHEN dss_ethnic.Description is null THEN 'UNKNOWN'
# MAGIC              WHEN mpi.NHSDEthnicity is null THEN 'UNKNOWN'
# MAGIC              ELSE mpi.NHSDEthnicity END as LowerEthnicity,
# MAGIC         CASE WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
# MAGIC              WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
# MAGIC              WHEN dss_ethnic.Description is null THEN 'UNKNOWN'
# MAGIC              ELSE dss_ethnic.Description END as LowerEthnicity_Desc,
# MAGIC       --THIS SECTION ADDS PROVIDER LEVEL DEMOGRAPHICS
# MAGIC         CASE WHEN mpi_prov.GenderIDCode IN ('1','2','3','4') THEN mpi_prov.GenderIDCode
# MAGIC             WHEN mpi_prov.Gender IN ('1','2','9') THEN mpi_prov.Gender
# MAGIC             ELSE 'UNKNOWN' END AS DER_GENDER_Prov,
# MAGIC         CASE WHEN mpi_prov.NHSDEthnicity = '99' THEN 'Not Known'
# MAGIC              WHEN mpi_prov.NHSDEthnicity = 'Z' THEN 'Not Stated'
# MAGIC              WHEN dss_ethnic_prov.Description is null THEN 'UNKNOWN'
# MAGIC              WHEN mpi_prov.NHSDEthnicity is null THEN 'UNKNOWN'
# MAGIC              ELSE mpi_prov.NHSDEthnicity END as LowerEthnicity_prov,
# MAGIC         CASE WHEN mpi_prov.NHSDEthnicity = '99' THEN 'Not Known'
# MAGIC              WHEN mpi_prov.NHSDEthnicity = 'Z' THEN 'Not Stated'
# MAGIC              WHEN dss_ethnic_prov.Description is null THEN 'UNKNOWN'
# MAGIC              ELSE dss_ethnic_prov.Description END as LowerEthnicity_Desc_prov
# MAGIC             
# MAGIC FROM (    SELECT *
# MAGIC           FROM $db_source.MHS101Referral as a
# MAGIC           LEFT JOIN $db_output.validcodes as vc
# MAGIC         --     join updated to evaluate validity at time of data rather than reporting month
# MAGIC             ON vc.datatable = 'mhs101referral' and vc.field = 'PrimReasonReferralMH' and vc.Measure = 'ASD' and vc.type = 'include' and A.PrimReasonReferralMH = vc.ValidValue
# MAGIC             and A.UniqMonthID >= vc.FirstMonth and (vc.LastMonth is null or A.UniqMonthID <= vc.LastMonth)
# MAGIC           WHERE ReferralRequestReceivedDate >= '2018-04-01'
# MAGIC             AND (RecordEndDate IS null OR RecordEndDate >= '$rp_enddate')
# MAGIC             AND RecordStartDate <= '$rp_enddate'
# MAGIC             AND vc.Measure is not null
# MAGIC --             AND PrimReasonReferralMH = '25'
# MAGIC       ) as ref   --This sub-query pulls all suspected autism referrals since April 1st 2018 when suspected autism referral reason introduced
# MAGIC   
# MAGIC LEFT JOIN $db_source.MHS001MPI as mpi
# MAGIC    ON ref.UniqMonthID = mpi.UniqMonthID
# MAGIC   AND ref.Person_ID = mpi.Person_ID -- Possibly one to look at as part of optimising
# MAGIC   AND mpi.PatMRecInRP = TRUE -- This is to get one person nationally so that the totals add up back to the national totals
# MAGIC   
# MAGIC LEFT JOIN $db_source.MHS001MPI as mpi_prov
# MAGIC    ON ref.UniqMonthID = mpi_prov.UniqMonthID
# MAGIC   AND ref.OrgIDProv = mpi_prov.OrgIDProv
# MAGIC   AND ref.Person_ID = mpi_prov.Person_ID -- This is to get the demographics for the person within each provider
# MAGIC   
# MAGIC LEFT JOIN $db_output.asd_CCG_LATEST ccg ON ref.Person_ID = ccg.Person_ID-- This gets one CCG/Sub ICB Location per Person_ID
# MAGIC 
# MAGIC LEFT JOIN $db_output.asd_STP_Mapping stp ON ccg.IC_Rec_CCG = stp.CCG_CODE
# MAGIC 
# MAGIC LEFT JOIN (SELECT *
# MAGIC           FROM $db_source.MHS501HospProvSpell
# MAGIC           WHERE StartDateHospProvSpell >= '2018-04-01'
# MAGIC             AND (RecordEndDate IS null OR RecordEndDate >= '$rp_enddate')
# MAGIC             AND RecordStartDate <= '$rp_enddate'
# MAGIC           ) as inp -- This sub query pulls out referrals that include an inpatient stay  
# MAGIC    ON ref.uniqservreqid = inp.uniqservreqid
# MAGIC   AND ref.person_id = inp.person_id
# MAGIC   
# MAGIC LEFT JOIN (SELECT PrimaryCode, Description FROM $dss_corporate.datadictionarycodes WHERE ItemName = 'ETHNIC_CATEGORY_CODE') dss_ethnic
# MAGIC      ON mpi.NHSDEthnicity = dss_ethnic.PrimaryCode
# MAGIC   
# MAGIC LEFT JOIN (SELECT PrimaryCode, Description FROM $dss_corporate.datadictionarycodes WHERE ItemName = 'ETHNIC_CATEGORY_CODE') dss_ethnic_prov
# MAGIC      ON mpi_prov.NHSDEthnicity = dss_ethnic_prov.PrimaryCode
# MAGIC   
# MAGIC WHERE inp.uniqservreqid is null -- This clause removes referrals that include an inpatient stay;
# MAGIC 
# MAGIC OPTIMIZE $db_output.Autism_12m_Referrals;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRUNCATE TABLE $db_output.Autism_12m_Referrals_ASD17;
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE $db_output.Autism_12m_Referrals_ASD17
# MAGIC 
# MAGIC SELECT  ref.Person_ID,
# MAGIC         ref.UniqServReqID,
# MAGIC         ref.UniqMonthID,
# MAGIC         ref.PrimReasonReferralMH,
# MAGIC         ref.RecordStartDate,
# MAGIC         ref.RecordEndDate,
# MAGIC         ref.ReferralRequestReceivedDate,
# MAGIC         ref.ServDischDate,
# MAGIC         ref.RecordNumber,
# MAGIC         ref.OrgIDProv,
# MAGIC         ccg.IC_Rec_CCG AS CCGCode,
# MAGIC         ccg.NAME AS CCGName,
# MAGIC         stp.STP_CODE AS STPCode,
# MAGIC         stp.STP_NAME AS STPName,
# MAGIC         stp.REGION_CODE AS RegionCode,
# MAGIC         stp.REGION_NAME AS RegionName,
# MAGIC       --THIS SECTION ADDS NATIONAL LEVEL DEMOGRAPHICS
# MAGIC         (CASE
# MAGIC             WHEN ref.AgeServReferRecDate between 0 and 9
# MAGIC             THEN 'Under 10'
# MAGIC             WHEN ref.AgeServReferRecDate between 10 and 17
# MAGIC             THEN '10 to 17'
# MAGIC             WHEN ref.AgeServReferRecDate between 18 and 24
# MAGIC             THEN '18 to 24'
# MAGIC             WHEN ref.AgeServReferRecDate between 25 and 34
# MAGIC             THEN '25 to 34'
# MAGIC             WHEN ref.AgeServReferRecDate between 35 and 44
# MAGIC             THEN '35 to 44'
# MAGIC             WHEN ref.AgeServReferRecDate between 45 and 54
# MAGIC             THEN '45 to 54'
# MAGIC             WHEN ref.AgeServReferRecDate between 55 and 64
# MAGIC             THEN '55 to 64'
# MAGIC             WHEN ref.AgeServReferRecDate > 64
# MAGIC             THEN '65 and Over'
# MAGIC             ELSE 'UNKNOWN'
# MAGIC         END) AS Age_Group,
# MAGIC         (CASE
# MAGIC             WHEN ref.AgeServReferRecDate is NULL
# MAGIC             THEN 'UNKNOWN'
# MAGIC             WHEN ref.AgeServReferRecDate between 0 and 17
# MAGIC             THEN 'Under 18'
# MAGIC             ELSE '18 and over'
# MAGIC         END) AS Age_Group_Higher,
# MAGIC         CASE WHEN mpi.GenderIDCode IN ('1','2','3','4') THEN mpi.GenderIDCode
# MAGIC             WHEN mpi.Gender IN ('1','2','9') THEN mpi.Gender
# MAGIC             ELSE 'UNKNOWN' END AS Der_Gender,
# MAGIC         CASE WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
# MAGIC              WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
# MAGIC              WHEN dss_ethnic.Description is null THEN 'UNKNOWN'
# MAGIC              WHEN mpi.NHSDEthnicity is null THEN 'UNKNOWN'
# MAGIC              ELSE mpi.NHSDEthnicity END as LowerEthnicity,
# MAGIC         CASE WHEN mpi.NHSDEthnicity = '99' THEN 'Not Known'
# MAGIC              WHEN mpi.NHSDEthnicity = 'Z' THEN 'Not Stated'
# MAGIC              WHEN dss_ethnic.Description is null THEN 'UNKNOWN'
# MAGIC              ELSE dss_ethnic.Description END as LowerEthnicity_Desc,
# MAGIC       --THIS SECTION ADDS PROVIDER LEVEL DEMOGRAPHICS
# MAGIC         CASE WHEN mpi_prov.GenderIDCode IN ('1','2','3','4') THEN mpi_prov.GenderIDCode
# MAGIC             WHEN mpi_prov.Gender IN ('1','2','9') THEN mpi_prov.Gender
# MAGIC             ELSE 'UNKNOWN' END AS DER_GENDER_Prov,
# MAGIC         CASE WHEN mpi_prov.NHSDEthnicity = '99' THEN 'Not Known'
# MAGIC              WHEN mpi_prov.NHSDEthnicity = 'Z' THEN 'Not Stated'
# MAGIC              WHEN dss_ethnic_prov.Description is null THEN 'UNKNOWN'
# MAGIC              WHEN mpi_prov.NHSDEthnicity is null THEN 'UNKNOWN'
# MAGIC              ELSE mpi_prov.NHSDEthnicity END as LowerEthnicity_prov,
# MAGIC         CASE WHEN mpi_prov.NHSDEthnicity = '99' THEN 'Not Known'
# MAGIC              WHEN mpi_prov.NHSDEthnicity = 'Z' THEN 'Not Stated'
# MAGIC              WHEN dss_ethnic_prov.Description is null THEN 'UNKNOWN'
# MAGIC              ELSE dss_ethnic_prov.Description END as LowerEthnicity_Desc_prov
# MAGIC FROM (    SELECT *
# MAGIC           FROM $db_source.MHS101Referral as a
# MAGIC           LEFT JOIN $db_output.validcodes as vc
# MAGIC         --     join updated to evaluate validity at time of data rather than reporting month
# MAGIC             ON vc.datatable = 'mhs101referral' and vc.field = 'PrimReasonReferralMH' and vc.Measure = 'ASD' and vc.type = 'include' and A.PrimReasonReferralMH = vc.ValidValue
# MAGIC             and A.UniqMonthID >= vc.FirstMonth and (vc.LastMonth is null or A.UniqMonthID <= vc.LastMonth)
# MAGIC           WHERE ReferralRequestReceivedDate >= '2018-04-01'
# MAGIC             AND vc.Measure is not null
# MAGIC --             AND PrimReasonReferralMH = '25'
# MAGIC       ) as ref   --This sub-query pulls all suspected autism referrals
# MAGIC   
# MAGIC LEFT JOIN $db_source.MHS001MPI as mpi
# MAGIC    ON ref.UniqMonthID = mpi.UniqMonthID
# MAGIC   AND ref.Person_ID = mpi.Person_ID
# MAGIC   AND mpi.PatMRecInRP = TRUE
# MAGIC   
# MAGIC LEFT JOIN $db_source.MHS001MPI as mpi_prov
# MAGIC    ON ref.UniqMonthID = mpi_prov.UniqMonthID
# MAGIC   AND ref.OrgIDProv = mpi_prov.OrgIDProv
# MAGIC   AND ref.Person_ID = mpi_prov.Person_ID
# MAGIC   
# MAGIC LEFT JOIN $db_output.asd_CCG_LATEST ccg ON ref.Person_ID = ccg.Person_ID
# MAGIC 
# MAGIC LEFT JOIN $db_output.asd_STP_Mapping stp ON ccg.IC_Rec_CCG = stp.CCG_CODE
# MAGIC 
# MAGIC LEFT JOIN (    SELECT *
# MAGIC           FROM $db_source.MHS501HospProvSpell
# MAGIC           WHERE StartDateHospProvSpell >= '2018-04-01'
# MAGIC       ) inp on ref.uniqservreqid = inp.uniqservreqid and ref.person_id = inp.person_id and ref.uniqmonthid = inp.uniqmonthid
# MAGIC   
# MAGIC LEFT JOIN (SELECT PrimaryCode, Description FROM $dss_corporate.datadictionarycodes WHERE ItemName = 'ETHNIC_CATEGORY_CODE') dss_ethnic
# MAGIC      ON mpi.NHSDEthnicity = dss_ethnic.PrimaryCode
# MAGIC   
# MAGIC LEFT JOIN (SELECT PrimaryCode, Description FROM $dss_corporate.datadictionarycodes WHERE ItemName = 'ETHNIC_CATEGORY_CODE') dss_ethnic_prov
# MAGIC      ON mpi_prov.NHSDEthnicity = dss_ethnic_prov.PrimaryCode
# MAGIC   
# MAGIC WHERE inp.uniqservreqid is null;
# MAGIC 
# MAGIC OPTIMIZE $db_output.Autism_12m_Referrals_ASD17;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- TRUNCATE TABLE $db_output.Diagnoses;
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE $db_output.Diagnoses
# MAGIC 
# MAGIC SELECT DISTINCT *
# MAGIC FROM
# MAGIC   (
# MAGIC 
# MAGIC   SELECT  m.*,
# MAGIC           pd.CodedDiagTimestamp as DiagDate,
# MAGIC           pd.Person_ID,
# MAGIC           pd.UniqServReqID,
# MAGIC           CASE WHEN c.CODE IS NOT NULL THEN 'Autism'
# MAGIC                WHEN c.CODE IS NULL and pd.PrimDiag like 'F%' THEN 'Other F'
# MAGIC                ELSE 'Other' END as Diag
# MAGIC   FROM $db_source.MHS604PrimDiag as pd
# MAGIC   INNER JOIN $db_output.Months as m
# MAGIC      ON pd.CodedDiagTimestamp between m.ReportingPeriodStartDate and m.ReportingPeriodEndDate
# MAGIC   LEFT JOIN $db_output.autismdiagcodes as c
# MAGIC      ON pd.PrimDiag = c.CODE
# MAGIC 
# MAGIC   UNION ALL
# MAGIC 
# MAGIC   SELECT m.*, pd.CodedDiagTimestamp as DiagDate, pd.Person_ID, pd.UniqServReqID,
# MAGIC           CASE WHEN c.CODE IS NOT NULL THEN 'Autism'
# MAGIC                WHEN c.CODE IS NULL and pd.SecDiag like 'F%' THEN 'Other F'
# MAGIC                ELSE 'Other' END as PrimDiag
# MAGIC   FROM $db_source.MHS605SecDiag as pd
# MAGIC   INNER JOIN $db_output.Months as m
# MAGIC      ON pd.CodedDiagTimestamp between m.ReportingPeriodStartDate and m.ReportingPeriodEndDate
# MAGIC   LEFT JOIN $db_output.autismdiagcodes as c
# MAGIC      ON pd.SecDiag = c.CODE
# MAGIC   ) AS a

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRUNCATE TABLE $db_output.Autism_Diagnoses;
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE $db_output.Autism_Diagnoses
# MAGIC 
# MAGIC SELECT  mnt.*,
# MAGIC         ref.UniqServReqID,
# MAGIC         ref.Person_ID,
# MAGIC         ref.ReferralRequestReceivedDate,
# MAGIC         ref.ServDischDate,
# MAGIC         ref.OrgIDProv,
# MAGIC         null as Name,
# MAGIC         ref.CCGCode,
# MAGIC         ref.CCGName,
# MAGIC         ref.STPCode,
# MAGIC         ref.STPName,
# MAGIC         ref.RegionCode,
# MAGIC         ref.RegionName,
# MAGIC         ref.Age_Group,
# MAGIC         ref.Age_Group_Higher,
# MAGIC         ref.DER_GENDER,
# MAGIC         ref.LowerEthnicity,
# MAGIC         ref.LowerEthnicity_Desc,
# MAGIC         ref.DER_GENDER_Prov,
# MAGIC         ref.LowerEthnicity_Prov,
# MAGIC         ref.LowerEthnicity_Desc_Prov,
# MAGIC         diag.diag
# MAGIC FROM $db_output.Autism_12m_Referrals as ref
# MAGIC CROSS JOIN $db_output.Months as mnt
# MAGIC INNER JOIN $db_output.Diagnoses as diag
# MAGIC    ON ref.Person_ID = diag.Person_ID
# MAGIC   AND ref.UniqServReqID = diag.UniqServReqID
# MAGIC   AND mnt.UniqMonthID = diag.UniqMonthID
# MAGIC WHERE ReferralRequestReceivedDate <= mnt.ReportingPeriodEndDate
# MAGIC   AND (ServDischDate is null or ServDischDate >= mnt.ReportingPeriodStartDate)
# MAGIC   AND (RecordEndDate is null or RecordEndDate >= mnt.ReportingPeriodEndDate);
# MAGIC   
# MAGIC OPTIMIZE $db_output.Autism_Diagnoses;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC -- TRUNCATE TABLE $db_output.carecontacts;
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE $db_output.carecontacts
# MAGIC 
# MAGIC SELECT  r.Person_ID,
# MAGIC         r.UniqServReqID,
# MAGIC         r.ConsMechanismMH,
# MAGIC         r.CareContDate
# MAGIC FROM
# MAGIC (
# MAGIC SELECT  r.Person_ID,
# MAGIC         r.UniqServReqID,
# MAGIC         C.ConsMechanismMH,
# MAGIC         C.CareContDate, 
# MAGIC         ROW_NUMBER() OVER (PARTITION BY C.Person_ID, C.UniqServReqID ORDER BY C.CareContDate ASC) as DateOrder 
# MAGIC FROM $db_output.Autism_12m_Referrals r
# MAGIC LEFT JOIN $db_source.MHS201CareContact C
# MAGIC    ON C.person_ID = r.person_ID
# MAGIC   AND C.UniqServReqID = r.UniqServReqID
# MAGIC LEFT JOIN $db_output.validcodes as vc
# MAGIC --     join updated to evaluate validity at time of data rather than reporting month
# MAGIC   ON vc.datatable = 'mhs201carecontact' and vc.field = 'AttendORDNACode' and vc.Measure = 'ASD' and vc.type = 'include' and C.AttendORDNACode = vc.ValidValue
# MAGIC   and C.UniqMonthID >= vc.FirstMonth and (vc.LastMonth is null or C.UniqMonthID <= vc.LastMonth)
# MAGIC LEFT JOIN $db_output.validcodes as vc2
# MAGIC --     join updated to evaluate validity at time of data rather than reporting month
# MAGIC   ON vc2.datatable = 'mhs201carecontact' and vc2.field = 'ConsMechanismMH' and vc2.Measure = 'ASD' and vc2.type = 'include' and C.ConsMechanismMH = vc2.ValidValue
# MAGIC   and C.UniqMonthID >= vc2.FirstMonth and (vc2.LastMonth is null or C.UniqMonthID <= vc2.LastMonth)
# MAGIC WHERE r.ReferralRequestReceivedDate <= C.CareContDate
# MAGIC   AND vc.Measure is not null
# MAGIC   AND vc2.Measure is not null
# MAGIC --   AND C.AttendORDNACode in (5,6)
# MAGIC --   AND ((r.UniqMonthID <= 1458 AND C.ConsMechanismMH in ('01','03'))
# MAGIC --         OR (r.UniqMonthID > 1458 AND C.ConsMechanismMH in ('01','11')))
# MAGIC ) r
# MAGIC WHERE DateOrder = 1
# MAGIC   AND CareContDate <= '$rp_enddate'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW RefTable_2 AS
# MAGIC 
# MAGIC SELECT ref.*, c.CareContDate, od.Name
# MAGIC FROM $db_output.Autism_12m_Referrals as ref
# MAGIC LEFT JOIN $db_output.carecontacts as c
# MAGIC    ON ref.Person_ID = c.Person_ID
# MAGIC   AND ref.UniqServReqID = c.UniqServReqID
# MAGIC LEFT JOIN $dss_corporate.org_daily as od
# MAGIC    ON ref.OrgIDProv = od.ORG_CODE
# MAGIC WHERE od.Org_is_current = '1'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRUNCATE TABLE $db_output.WaitTimes;
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE $db_output.WaitTimes
# MAGIC 
# MAGIC SELECT  mth.*,
# MAGIC         ref.Person_ID,
# MAGIC         ref.CareContDate,
# MAGIC         ref.ReferralRequestReceivedDate,
# MAGIC         ref.ServDischDate,
# MAGIC         ref.OrgIDProv,
# MAGIC         null as Name,
# MAGIC         ref.CCGCode,
# MAGIC         ref.CCGName,
# MAGIC         ref.STPCode,
# MAGIC         ref.STPName,
# MAGIC         ref.RegionCode,
# MAGIC         ref.RegionName,
# MAGIC         ref.Age_Group,
# MAGIC         ref.Age_Group_Higher,
# MAGIC         ref.DER_GENDER,
# MAGIC         ref.LowerEthnicity,
# MAGIC         ref.LowerEthnicity_Desc,
# MAGIC         ref.DER_GENDER_Prov,
# MAGIC         ref.LowerEthnicity_Prov,
# MAGIC         ref.LowerEthnicity_Desc_Prov,
# MAGIC         datediff(CareContDate,ReferralRequestReceivedDate) as waittime,
# MAGIC         CASE WHEN datediff(CareContDate,ReferralRequestReceivedDate) IS NULL THEN 'No Contact'
# MAGIC              WHEN datediff(CareContDate,ReferralRequestReceivedDate)<=91 THEN 'Less than 13 weeks'
# MAGIC              WHEN datediff(CareContDate,ReferralRequestReceivedDate)>91 THEN 'More than 13 weeks'
# MAGIC              ELSE 'Unknown' END as WaitLength,
# MAGIC         CASE WHEN datediff(COALESCE(CareContDate,ReportingPeriodEndDate),ReferralRequestReceivedDate)<=91 THEN 'N'
# MAGIC              WHEN datediff(COALESCE(CareContDate,ReportingPeriodEndDate),ReferralRequestReceivedDate)>91 THEN 'Y'
# MAGIC              ELSE 'Unknown' END as Wait13WeeksPlus,
# MAGIC         CASE WHEN datediff(COALESCE(CareContDate,ReportingPeriodEndDate),ReferralRequestReceivedDate)<=182 THEN 'N'
# MAGIC              WHEN datediff(COALESCE(CareContDate,ReportingPeriodEndDate),ReferralRequestReceivedDate)>182 THEN 'Y'
# MAGIC              ELSE 'Unknown' END as Wait26WeeksPlus,
# MAGIC         CASE WHEN datediff(COALESCE(CareContDate,ReportingPeriodEndDate),ReferralRequestReceivedDate)<=365 THEN 'N'
# MAGIC              WHEN datediff(COALESCE(CareContDate,ReportingPeriodEndDate),ReferralRequestReceivedDate)>365 THEN 'Y'
# MAGIC              ELSE 'Unknown' END as Wait12MonthsPlus
# MAGIC FROM RefTable_2 as ref
# MAGIC INNER JOIN $db_output.referral_months as mth ON ref.UniqServReqID = mth.UniqServReqID 
# MAGIC WHERE ReferralRequestReceivedDate <= ReportingPeriodEndDate
# MAGIC   AND (ServDischDate is null or ServDischDate >= ReportingPeriodStartDate)
# MAGIC   AND (RecordEndDate is null or RecordEndDate >= ReportingPeriodEndDate);
# MAGIC --   AND od.Org_is_current = '1';
# MAGIC   
# MAGIC OPTIMIZE $db_output.WaitTimes;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE $db_output.asd_output