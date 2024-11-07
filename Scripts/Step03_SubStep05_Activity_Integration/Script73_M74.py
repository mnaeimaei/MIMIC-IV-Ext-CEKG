import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M74_SAPSII` ;
create table `your-project-id1234.M74_SAPSII.TabA1` as
SELECT * FROM `your-project-id1234.S_Derived1.sapsii` ;



create table `your-project-id1234.M74_SAPSII.Temp`   as
Select * from `your-project-id1234.M74_SAPSII.TabA1`  ;

ALTER TABLE `your-project-id1234.M74_SAPSII.Temp`
ADD column  c1 STRING ;

UPDATE `your-project-id1234.M74_SAPSII.Temp`
SET c1 ="A"
WHERE c1 is null ;

create table `your-project-id1234.M74_SAPSII.TabA2`   as
Select
subject_id, hadm_id, stay_id, starttime, endtime, sapsii, sapsii_prob, age_score, hr_score, sysbp_score, temp_score, PaO2FiO2_score, uo_score, bun_score, wbc_score, potassium_score, sodium_score, bicarbonate_score, bilirubin_score, gcs_score, comorbidity_score, admissiontype_score,Row_number() over (partition by c1 order by subject_id) as SAPSII_ID,
from `your-project-id1234.M74_SAPSII.Temp`  ;

drop table `your-project-id1234.M74_SAPSII.Temp`  ;



alter table `your-project-id1234.M74_SAPSII.TabA2`
add column ss string;

alter table `your-project-id1234.M74_SAPSII.TabA2`
add column ee string;

update `your-project-id1234.M74_SAPSII.TabA2`
set ss="start"
where ss is null and starttime is not null;

update `your-project-id1234.M74_SAPSII.TabA2`
set ee="end"
where ee is null and endtime is not null;



create table  `your-project-id1234.M74_SAPSII.TabA3`   as
select subject_id, hadm_id, stay_id, starttime as Timestamps , SAPSII_ID,ss as SAPSII_Status, sapsii, sapsii_prob, age_score, hr_score, sysbp_score, temp_score, PaO2FiO2_score, uo_score, bun_score, wbc_score, potassium_score, sodium_score, bicarbonate_score, bilirubin_score, gcs_score, comorbidity_score, admissiontype_score
FROM `your-project-id1234.M74_SAPSII.TabA2`

union all

select subject_id, hadm_id, stay_id, endtime as Timestamps, SAPSII_ID, ee  as SAPSII_Status , sapsii, sapsii_prob, age_score, hr_score, sysbp_score, temp_score, PaO2FiO2_score, uo_score, bun_score, wbc_score, potassium_score, sodium_score, bicarbonate_score, bilirubin_score, gcs_score, comorbidity_score, admissiontype_score
FROM `your-project-id1234.M74_SAPSII.TabA2`  ;



CREATE TABLE  `your-project-id1234.M74_SAPSII.TabA4`   AS
SELECT distinct   
subject_id, hadm_id, Timestamps, SAPSII_ID, SAPSII_Status, sapsii, sapsii_prob, age_score, hr_score, sysbp_score, temp_score, PaO2FiO2_score, uo_score, bun_score, wbc_score, potassium_score, sodium_score, bicarbonate_score, bilirubin_score, gcs_score, comorbidity_score, admissiontype_score  
From `your-project-id1234.M74_SAPSII.TabA3` ;



ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M74_SAPSII.TabA4`
SET Activity_Synonym ="SAPSII"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M74_SAPSII.TabA4`
SET Activity ="Simplified Acute Physiology Scores"
WHERE Activity is null ;





ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f1 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f1="SAPSII_ID" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f2 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f2="SAPSII_Status" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f3 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f3="sapsii" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f4 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f4="sapsii_prob" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f5 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f5="age_score" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f6 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f6="hr_score" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f7 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f7="sysbp_score" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f8 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f8="temp_score" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f9 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f9="PaO2FiO2_score" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f10 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f10="uo_score" 
where f10 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f11 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f11="bun_score" 
where f11 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f12 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f12="wbc_score" 
where f12 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f13 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f13="potassium_score" 
where f13 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f14 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f14 ="sodium_score" 
where f14 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f15 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f15 ="bicarbonate_score" 
where f15 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f16 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f16 ="bilirubin_score" 
where f16 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f17 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f17 ="gcs_score" 
where f17 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f18 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f18 ="comorbidity_score" 
where f18 is null;
#################################################
ALTER TABLE `your-project-id1234.M74_SAPSII.TabA4`
ADD column  f19 STRING ;
update `your-project-id1234.M74_SAPSII.TabA4`
set f19 ="admissiontype_score" 
where f19 is null;
#################################################
################################################################

create table `your-project-id1234.M74_SAPSII.TabA5`   as
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(SAPSII_ID AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(SAPSII_Status AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(sapsii AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(sapsii_prob AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(age_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(hr_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(sysbp_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(temp_score AS STRING) as  value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(PaO2FiO2_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(uo_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f11 as feature , CAST(bun_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f12 as feature , CAST(wbc_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f13 as feature , CAST(potassium_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f14 as feature , CAST(sodium_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f15 as feature , CAST(bicarbonate_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f16 as feature , CAST(bilirubin_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f17 as feature , CAST(gcs_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f18 as feature , CAST(comorbidity_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f19 as feature , CAST(admissiontype_score AS STRING) as value
from  `your-project-id1234.M74_SAPSII.TabA4`  ;
#################################################
alter table `your-project-id1234.M74_SAPSII.TabA5`
add column num INT64;
update`your-project-id1234.M74_SAPSII.TabA5`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M74_SAPSII.TabA6`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M74_SAPSII.TabA5`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M74_SAPSII.TabA5`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;




ALTER TABLE `your-project-id1234.M74_SAPSII.TabA6`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M74_SAPSII.TabA6`
SET Activity_Value_ID = concat("sapsii",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M74_SAPSII.TabA7`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M74_SAPSII.TabA6`  ;

CREATE TABLE `your-project-id1234.M74_SAPSII.TabA8`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M74_SAPSII.TabA6`  ;



ALTER TABLE `your-project-id1234.M74_SAPSII.TabA8`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M74_SAPSII.TabA8`
SET Activity_Synonym = "SAPSII"
WHERE Activity_Synonym is null ;

ALTER TABLE `your-project-id1234.M74_SAPSII.TabA8`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M74_SAPSII.TabA8`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M74_SAPSII.TabA9`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M74_SAPSII.TabA8`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M74_SAPSII.TabA8`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M74_SAPSII.TabA10`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M74_SAPSII.TabA9`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M74_SAPSII.TabA11`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M74_SAPSII.TabA10`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M74_SAPSII.TabA12`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M74_SAPSII.TabA7`   as a
LEFT JOIN `your-project-id1234.M74_SAPSII.TabA11`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M74_SAPSII.TabA13`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M74_SAPSII.TabA9`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M74_SAPSII.TabA14`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M74_SAPSII.TabA8`  )    ;



CREATE TABLE `your-project-id1234.M74_SAPSII.TabA15`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M74_SAPSII.TabA10`    as a
LEFT JOIN   `your-project-id1234.M74_SAPSII.TabA11`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;





'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
