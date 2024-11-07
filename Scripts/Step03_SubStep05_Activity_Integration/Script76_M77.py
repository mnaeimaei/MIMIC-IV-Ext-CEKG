import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M77_VASO` ;
create table `your-project-id1234.M77_VASO.TabA1` as
SELECT *  FROM `your-project-id1234.N06_VASO.TabA61` ;



create table `your-project-id1234.M77_VASO.Temp`   as
Select * from `your-project-id1234.M77_VASO.TabA1`  ;

ALTER TABLE `your-project-id1234.M77_VASO.Temp`
ADD column  c1 STRING ;

UPDATE `your-project-id1234.M77_VASO.Temp`
SET c1 ="A"
WHERE c1 is null ;

create table `your-project-id1234.M77_VASO.TabA2`   as
Select
subject_id, hadm_id, stay_id,  Row_number() over (partition by c1 order by subject_id) as adminsitrationID,  solutionID, linkorderid, drug, drug_amount, drug_amount_uom, original_drug_rate, drug_rate, drug_rate_uom, status_description, starttime, endtime, dobutamine_vaso_rate, dobutamine_vaso_amount, dopamine_vaso_rate, dopamine_vaso_amount, epinephrine_vaso_rate, epinephrine_vaso_amount, milrinone_vaso_rate, milrinone_vaso_amount, norepinephrine_vaso_rate, norepinephrine_vaso_amount, norepinephrine_equivalent_dose, phenylephrine_vaso_rate, phenylephrine_vaso_amount, vasopressin_vaso_rate, vasopressin_vaso_amount
from `your-project-id1234.M77_VASO.Temp`  ;

drop table `your-project-id1234.M77_VASO.Temp`  ;



alter table `your-project-id1234.M77_VASO.TabA2`
add column ss string;

alter table `your-project-id1234.M77_VASO.TabA2`
add column ee string;

update `your-project-id1234.M77_VASO.TabA2`
set ss="start"
where ss is null and starttime is not null;

update `your-project-id1234.M77_VASO.TabA2`
set ee="end"
where ee is null and endtime is not null;




CREATE TABLE  `your-project-id1234.M77_VASO.TabA3`  as 
SELECT subject_id, hadm_id, stay_id, 
starttime as Timestamps , 
adminsitrationID , 
ss as administration_Status , 
solutionID, drug, drug_amount, drug_amount_uom, original_drug_rate, drug_rate, drug_rate_uom, status_description, dobutamine_vaso_rate, dobutamine_vaso_amount, dopamine_vaso_rate, dopamine_vaso_amount, epinephrine_vaso_rate, epinephrine_vaso_amount, milrinone_vaso_rate, milrinone_vaso_amount, norepinephrine_vaso_rate, norepinephrine_vaso_amount, norepinephrine_equivalent_dose, phenylephrine_vaso_rate, phenylephrine_vaso_amount, vasopressin_vaso_rate, vasopressin_vaso_amount
FROM `your-project-id1234.M77_VASO.TabA2`  
union all
SELECT subject_id, hadm_id, stay_id, 
endtime as Timestamps , 
adminsitrationID , 
ee  as administration_Status , 
solutionID, drug, drug_amount, drug_amount_uom, original_drug_rate, drug_rate, drug_rate_uom, status_description, dobutamine_vaso_rate, dobutamine_vaso_amount, dopamine_vaso_rate, dopamine_vaso_amount, epinephrine_vaso_rate, epinephrine_vaso_amount, milrinone_vaso_rate, milrinone_vaso_amount, norepinephrine_vaso_rate, norepinephrine_vaso_amount, norepinephrine_equivalent_dose, phenylephrine_vaso_rate, phenylephrine_vaso_amount, vasopressin_vaso_rate, vasopressin_vaso_amount
FROM `your-project-id1234.M77_VASO.TabA2`  ;



CREATE TABLE  `your-project-id1234.M77_VASO.TabA4`   AS
SELECT distinct   
subject_id, hadm_id, Timestamps, adminsitrationID, administration_Status, solutionID, drug, drug_amount, drug_amount_uom, original_drug_rate, drug_rate, drug_rate_uom, status_description, dobutamine_vaso_rate, dobutamine_vaso_amount, dopamine_vaso_rate, dopamine_vaso_amount, epinephrine_vaso_rate, epinephrine_vaso_amount, milrinone_vaso_rate, milrinone_vaso_amount, norepinephrine_vaso_rate, norepinephrine_vaso_amount, norepinephrine_equivalent_dose, phenylephrine_vaso_rate, phenylephrine_vaso_amount, vasopressin_vaso_rate, vasopressin_vaso_amount  
From `your-project-id1234.M77_VASO.TabA3` ;



ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M77_VASO.TabA4`
SET Activity_Synonym ="VASO"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M77_VASO.TabA4`
SET Activity ="Specific_Vasopressor_inotropic_medication_administration"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f1 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f1="adminsitrationID" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f2 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f2="administration_Status" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f3 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f3="solutionID" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f4 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f4="drug" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f5 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f5="drug_amount" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f6 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f6="drug_amount_uom" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f7 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f7="original_drug_rate" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f8 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f8="drug_rate" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f9 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f9="drug_rate_uom" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f10 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f10="status_description" 
where f10 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f11 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f11="dobutamine_vaso_rate" 
where f11 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f12 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f12="dobutamine_vaso_amount" 
where f12 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f13 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f13="dopamine_vaso_rate" 
where f13 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f14 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f14 ="dopamine_vaso_amount" 
where f14 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f15 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f15 ="epinephrine_vaso_rate" 
where f15 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f16 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f16 ="epinephrine_vaso_amount" 
where f16 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f17 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f17 ="milrinone_vaso_rate" 
where f17 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f18 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f18 ="milrinone_vaso_amount" 
where f18 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f19 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f19 ="norepinephrine_vaso_rate" 
where f19 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f20 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f20 ="norepinephrine_vaso_amount" 
where f20 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f21 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f21 ="norepinephrine_equivalent_dose" 
where f21 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f22 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f22 ="phenylephrine_vaso_rate" 
where f22 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f23 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f23 ="phenylephrine_vaso_amount" 
where f23 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f24 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f24 ="vasopressin_vaso_rate" 
where f24 is null;
#################################################
ALTER TABLE `your-project-id1234.M77_VASO.TabA4`
ADD column  f25 STRING ;
update `your-project-id1234.M77_VASO.TabA4`
set f25 ="vasopressin_vaso_amount" 
where f25 is null;
#################################################
create table `your-project-id1234.M77_VASO.TabA5`   as
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(adminsitrationID AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(administration_Status AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(solutionID AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(drug AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(drug_amount AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(drug_amount_uom AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(original_drug_rate AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(drug_rate AS STRING) as  value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(drug_rate_uom AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(status_description AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f11 as feature , CAST(dobutamine_vaso_rate AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f12 as feature , CAST(dobutamine_vaso_amount AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f13 as feature , CAST(dopamine_vaso_rate AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f14 as feature , CAST(dopamine_vaso_amount AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f15 as feature , CAST(epinephrine_vaso_rate AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f16 as feature , CAST(epinephrine_vaso_amount AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f17 as feature , CAST(milrinone_vaso_rate AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f18 as feature , CAST(milrinone_vaso_amount AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f19 as feature , CAST(norepinephrine_vaso_rate AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f20 as feature , CAST(norepinephrine_vaso_amount AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f21 as feature , CAST(norepinephrine_equivalent_dose AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f22 as feature , CAST(phenylephrine_vaso_rate AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f23 as feature , CAST(phenylephrine_vaso_amount AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f24 as feature , CAST(vasopressin_vaso_rate AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f25 as feature , CAST(vasopressin_vaso_amount AS STRING) as value
from  `your-project-id1234.M77_VASO.TabA4` ;
#################################################
alter table `your-project-id1234.M77_VASO.TabA5`
add column num INT64;
update`your-project-id1234.M77_VASO.TabA5`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M77_VASO.TabA6`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M77_VASO.TabA5`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M77_VASO.TabA5`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;





ALTER TABLE `your-project-id1234.M77_VASO.TabA6`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M77_VASO.TabA6`
SET Activity_Value_ID = concat("vaso",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M77_VASO.TabA7`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M77_VASO.TabA6`  ;

CREATE TABLE `your-project-id1234.M77_VASO.TabA8`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M77_VASO.TabA6`  ;




ALTER TABLE `your-project-id1234.M77_VASO.TabA8`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M77_VASO.TabA8`
SET Activity_Synonym = "VASO"
WHERE Activity_Synonym is null ;


ALTER TABLE `your-project-id1234.M77_VASO.TabA8`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M77_VASO.TabA8`
SET num = 1
WHERE num is null ;




CREATE TABLE `your-project-id1234.M77_VASO.TabA9`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M77_VASO.TabA8`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M77_VASO.TabA8`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M77_VASO.TabA10`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M77_VASO.TabA9`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M77_VASO.TabA11`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M77_VASO.TabA10`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M77_VASO.TabA12`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M77_VASO.TabA7`   as a
LEFT JOIN `your-project-id1234.M77_VASO.TabA11`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M77_VASO.TabA13`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M77_VASO.TabA9`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M77_VASO.TabA14`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M77_VASO.TabA8`  )    ;



CREATE TABLE `your-project-id1234.M77_VASO.TabA15`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M77_VASO.TabA10`    as a
LEFT JOIN   `your-project-id1234.M77_VASO.TabA11`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;





'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
