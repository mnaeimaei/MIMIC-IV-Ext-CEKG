import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M93_SOFA` ;
create table `your-project-id1234.M93_SOFA.TabA1` as
SELECT * FROM `your-project-id1234.N16_sofa.sofa_subject` ;



create table `your-project-id1234.M93_SOFA.Temp`   as
Select * from `your-project-id1234.M93_SOFA.TabA1`  ;

ALTER TABLE `your-project-id1234.M93_SOFA.Temp`
ADD column  c1 STRING ;

UPDATE `your-project-id1234.M93_SOFA.Temp`
SET c1 ="A"
WHERE c1 is null ;

create table `your-project-id1234.M93_SOFA.TabA2`   as
Select
subject_id, stay_id, hr, starttime, endtime, Row_number() over (partition by c1 order by subject_id) as SOFA_ID, pao2fio2ratio_novent, pao2fio2ratio_vent, rate_epinephrine, rate_norepinephrine, rate_dopamine, rate_dobutamine, meanbp_min, gcs_min, uo_24hr, bilirubin_max, creatinine_max, platelet_min, respiration, coagulation, liver, cardiovascular, cns, renal, respiration_24hours, coagulation_24hours, liver_24hours, cardiovascular_24hours, cns_24hours, renal_24hours, sofa_24hours
from `your-project-id1234.M93_SOFA.Temp`  ;

drop table `your-project-id1234.M93_SOFA.Temp`  ;



alter table `your-project-id1234.M93_SOFA.TabA2`
add column ss string;

alter table `your-project-id1234.M93_SOFA.TabA2`
add column ee string;

update `your-project-id1234.M93_SOFA.TabA2`
set ss="start"
where ss is null and starttime is not null;

update `your-project-id1234.M93_SOFA.TabA2`
set ee="end"
where ee is null and endtime is not null;




CREATE TABLE  `your-project-id1234.M93_SOFA.TabA3`  as
SELECT subject_id, stay_id ,
starttime as Timestamps , 
SOFA_ID , 
ss as Invasive_Line_Status , 
hr, pao2fio2ratio_novent, pao2fio2ratio_vent, rate_epinephrine, rate_norepinephrine, rate_dopamine, rate_dobutamine, meanbp_min, gcs_min, uo_24hr, bilirubin_max, creatinine_max, platelet_min, respiration, coagulation, liver, cardiovascular, cns, renal, respiration_24hours, coagulation_24hours, liver_24hours, cardiovascular_24hours, cns_24hours, renal_24hours, sofa_24hours
FROM `your-project-id1234.M93_SOFA.TabA2`  
union all
SELECT subject_id, stay_id ,
endtime as Timestamps , 
SOFA_ID , 
ee  as Invasive_Line_Status , 
hr, pao2fio2ratio_novent, pao2fio2ratio_vent, rate_epinephrine, rate_norepinephrine, rate_dopamine, rate_dobutamine, meanbp_min, gcs_min, uo_24hr, bilirubin_max, creatinine_max, platelet_min, respiration, coagulation, liver, cardiovascular, cns, renal, respiration_24hours, coagulation_24hours, liver_24hours, cardiovascular_24hours, cns_24hours, renal_24hours, sofa_24hours
FROM `your-project-id1234.M93_SOFA.TabA2`  ;



CREATE TABLE  `your-project-id1234.M93_SOFA.TabA4`   AS
SELECT distinct * FROM (
SELECT
a.subject_id ,b.hadm_id, a.stay_id , a.Timestamps , a.SOFA_ID , a.Invasive_Line_Status , a.hr , a.pao2fio2ratio_novent , a.pao2fio2ratio_vent , a.rate_epinephrine , a.rate_norepinephrine , a.rate_dopamine , a.rate_dobutamine , a.meanbp_min , a.gcs_min , a.uo_24hr , a.bilirubin_max , a.creatinine_max , a.platelet_min , a.respiration , a.coagulation , a.liver , a.cardiovascular , a.cns , a.renal , a.respiration_24hours , a.coagulation_24hours , a.liver_24hours , a.cardiovascular_24hours , a.cns_24hours , a.renal_24hours , a.sofa_24hours,
From `your-project-id1234.M93_SOFA.TabA3`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SHI`   as b
ON
a.subject_id=b.subject_id AND a.stay_id=b.stay_id 
)
; 



CREATE TABLE  `your-project-id1234.M93_SOFA.TabA5`   AS
SELECT distinct   
subject_id, hadm_id, Timestamps, SOFA_ID, Invasive_Line_Status, hr, pao2fio2ratio_novent, pao2fio2ratio_vent, rate_epinephrine, rate_norepinephrine, rate_dopamine, rate_dobutamine, meanbp_min, gcs_min, uo_24hr, bilirubin_max, creatinine_max, platelet_min, respiration, coagulation, liver, cardiovascular, cns, renal, respiration_24hours, coagulation_24hours, liver_24hours, cardiovascular_24hours, cns_24hours, renal_24hours, sofa_24hours  
From `your-project-id1234.M93_SOFA.TabA4` ;



ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M93_SOFA.TabA5`
SET Activity_Synonym ="SOFA"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M93_SOFA.TabA5`
SET Activity ="Sequential_Organ_Failure_Assessment"
WHERE Activity is null ;




ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f1 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f1="SOFA_ID" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f2 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f2="Invasive_Line_Status" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f3 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f3="hr" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f4 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f4="pao2fio2ratio_novent" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f5 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f5="pao2fio2ratio_vent" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f6 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f6="rate_epinephrine" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f7 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f7="rate_norepinephrine" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f8 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f8="rate_dopamine" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f9 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f9="rate_dobutamine" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f10 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f10="meanbp_min" 
where f10 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f11 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f11="gcs_min" 
where f11 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f12 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f12="uo_24hr" 
where f12 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f13 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f13="bilirubin_max" 
where f13 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f14 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f14 ="creatinine_max" 
where f14 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f15 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f15 ="platelet_min" 
where f15 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f16 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f16 ="respiration" 
where f16 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f17 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f17 ="coagulation" 
where f17 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f18 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f18 ="liver" 
where f18 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f19 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f19 ="cardiovascular" 
where f19 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f20 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f20 ="cns" 
where f20 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f21 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f21 ="renal" 
where f21 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f22 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f22 ="respiration_24hours" 
where f22 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f23 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f23 ="coagulation_24hours" 
where f23 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f24 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f24 ="liver_24hours" 
where f24 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f25 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f25 ="cardiovascular_24hours" 
where f25 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f26 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f26="cns_24hours" 
where f26 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f27 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f27="renal_24hours" 
where f27 is null;
#################################################
ALTER TABLE `your-project-id1234.M93_SOFA.TabA5`
ADD column  f28 STRING ;
update `your-project-id1234.M93_SOFA.TabA5`
set f28="sofa_24hours" 
where f28 is null;
################################################################

create table `your-project-id1234.M93_SOFA.TabA6`   as
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(SOFA_ID AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(Invasive_Line_Status AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(hr AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(pao2fio2ratio_novent AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(pao2fio2ratio_vent AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(rate_epinephrine AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(rate_norepinephrine AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(rate_dopamine AS STRING) as  value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(rate_dobutamine AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(meanbp_min AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f11 as feature , CAST(gcs_min AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f12 as feature , CAST(uo_24hr AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f13 as feature , CAST(bilirubin_max AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f14 as feature , CAST(creatinine_max AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f15 as feature , CAST(platelet_min AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f16 as feature , CAST(respiration AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f17 as feature , CAST(coagulation AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f18 as feature , CAST(liver AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f19 as feature , CAST(cardiovascular AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f20 as feature , CAST(cns AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f21 as feature , CAST(renal AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f22 as feature , CAST(respiration_24hours AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f23 as feature , CAST(coagulation_24hours AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f24 as feature , CAST(liver_24hours AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f25 as feature , CAST(cardiovascular_24hours AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f26 as feature , CAST(cns_24hours AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f27 as feature , CAST(renal_24hours AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f28 as feature , CAST(sofa_24hours AS STRING) as value
from  `your-project-id1234.M93_SOFA.TabA5`   ;
#################################################
alter table `your-project-id1234.M93_SOFA.TabA6`
add column num INT64;
update`your-project-id1234.M93_SOFA.TabA6`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M93_SOFA.TabA7`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M93_SOFA.TabA6`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M93_SOFA.TabA6`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;






ALTER TABLE `your-project-id1234.M93_SOFA.TabA7`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M93_SOFA.TabA7`
SET Activity_Value_ID = concat("sofa",RankN)
WHERE Activity_Value_ID is null ;



CREATE TABLE `your-project-id1234.M93_SOFA.TabA8`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M93_SOFA.TabA7`  ;

CREATE TABLE `your-project-id1234.M93_SOFA.TabA9`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M93_SOFA.TabA7`  ;




ALTER TABLE `your-project-id1234.M93_SOFA.TabA9`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M93_SOFA.TabA9`
SET Activity_Synonym = "SOFA"
WHERE Activity_Synonym is null ;

ALTER TABLE `your-project-id1234.M93_SOFA.TabA9`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M93_SOFA.TabA9`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M93_SOFA.TabA10`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M93_SOFA.TabA9`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M93_SOFA.TabA9`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M93_SOFA.TabA11`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M93_SOFA.TabA10`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M93_SOFA.TabA12`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M93_SOFA.TabA11`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M93_SOFA.TabA13`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M93_SOFA.TabA8`   as a
LEFT JOIN `your-project-id1234.M93_SOFA.TabA12`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M93_SOFA.TabA14`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M93_SOFA.TabA10`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M93_SOFA.TabA15`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M93_SOFA.TabA9`  )    ;



CREATE TABLE `your-project-id1234.M93_SOFA.TabA16`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M93_SOFA.TabA11`    as a
LEFT JOIN   `your-project-id1234.M93_SOFA.TabA12`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;






'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
