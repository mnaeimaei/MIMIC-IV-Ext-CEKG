import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M60_GVASO` ;
create table `your-project-id1234.M60_GVASO.TabA1` as
SELECT *  FROM `your-project-id1234.N06_VASO.TabA62` ;



create table `your-project-id1234.M60_GVASO.Temp`   as
Select * from `your-project-id1234.M60_GVASO.TabA1`  ;

ALTER TABLE `your-project-id1234.M60_GVASO.Temp`
ADD column  c1 STRING ;

UPDATE `your-project-id1234.M60_GVASO.Temp`
SET c1 ="A"
WHERE c1 is null ;

create table `your-project-id1234.M60_GVASO.TabA2`   as
Select
stay_id, starttime, endtime, Row_number() over (partition by c1 order by stay_id) as administrationID,  dobutamine_vaso_rate, dobutamine_vaso_amount, dopamine_vaso_rate, dopamine_vaso_amount, epinephrine_vaso_rate, epinephrine_vaso_amount, milrinone_vaso_rate, milrinone_vaso_amount, norepinephrine_vaso_rate, norepinephrine_vaso_amount, norepinephrine_equivalent_dose, phenylephrine_vaso_rate, phenylephrine_vaso_amount, vasopressin_vaso_rate, vasopressin_vaso_amount
from `your-project-id1234.M60_GVASO.Temp`  ;

drop table `your-project-id1234.M60_GVASO.Temp`  ;



alter table `your-project-id1234.M60_GVASO.TabA2`
add column ss string;
alter table `your-project-id1234.M60_GVASO.TabA2`
add column ee string;
update `your-project-id1234.M60_GVASO.TabA2`
set ss="start"
where ss is null and starttime is not null;
update `your-project-id1234.M60_GVASO.TabA2`
set ee="end"
where ee is null and endtime is not null;




CREATE TABLE  `your-project-id1234.M60_GVASO.TabA3`  as
SELECT stay_id ,
starttime as Timestamps , 
administrationID , 
ss as administration_Status , 
dobutamine_vaso_rate, dobutamine_vaso_amount, dopamine_vaso_rate, dopamine_vaso_amount, epinephrine_vaso_rate, epinephrine_vaso_amount, milrinone_vaso_rate, milrinone_vaso_amount, norepinephrine_vaso_rate, norepinephrine_vaso_amount, norepinephrine_equivalent_dose, phenylephrine_vaso_rate, phenylephrine_vaso_amount, vasopressin_vaso_rate, vasopressin_vaso_amount
FROM `your-project-id1234.M60_GVASO.TabA2`  
union all
SELECT stay_id ,
endtime as Timestamps , 
administrationID , 
ee  as administration_Status , 
dobutamine_vaso_rate, dobutamine_vaso_amount, dopamine_vaso_rate, dopamine_vaso_amount, epinephrine_vaso_rate, epinephrine_vaso_amount, milrinone_vaso_rate, milrinone_vaso_amount, norepinephrine_vaso_rate, norepinephrine_vaso_amount, norepinephrine_equivalent_dose, phenylephrine_vaso_rate, phenylephrine_vaso_amount, vasopressin_vaso_rate, vasopressin_vaso_amount
FROM `your-project-id1234.M60_GVASO.TabA2`  ;



CREATE TABLE  `your-project-id1234.M60_GVASO.TabA4`   AS
SELECT distinct * FROM (
SELECT
b.subject_id , b.hadm_id, a.stay_id , a.Timestamps , a.administrationID , a.administration_Status , a.dobutamine_vaso_rate , a.dobutamine_vaso_amount , a.dopamine_vaso_rate , a.dopamine_vaso_amount , a.epinephrine_vaso_rate , a.epinephrine_vaso_amount , a.milrinone_vaso_rate , a.milrinone_vaso_amount , a.norepinephrine_vaso_rate , a.norepinephrine_vaso_amount , a.norepinephrine_equivalent_dose , a.phenylephrine_vaso_rate , a.phenylephrine_vaso_amount , a.vasopressin_vaso_rate , a.vasopressin_vaso_amount,

From `your-project-id1234.M60_GVASO.TabA3`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SHI`   as b
ON
a.stay_id=b.stay_id
)
;



CREATE TABLE  `your-project-id1234.M60_GVASO.TabA5`   AS
SELECT distinct 
subject_id, hadm_id, Timestamps, administrationID, administration_Status, dobutamine_vaso_rate, dobutamine_vaso_amount, dopamine_vaso_rate, dopamine_vaso_amount, epinephrine_vaso_rate, epinephrine_vaso_amount, milrinone_vaso_rate, milrinone_vaso_amount, norepinephrine_vaso_rate, norepinephrine_vaso_amount, norepinephrine_equivalent_dose, phenylephrine_vaso_rate, phenylephrine_vaso_amount, vasopressin_vaso_rate, vasopressin_vaso_amount
From `your-project-id1234.M60_GVASO.TabA4` ;



ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M60_GVASO.TabA5`
SET Activity_Synonym ="GVASO"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M60_GVASO.TabA5`
SET Activity ="Generic_Vasopressor_inotropic_medication_administration"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f1 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f1="administrationID" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f2 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f2="administration_Status" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f3 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f3="dobutamine_vaso_rate" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f4 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f4="dobutamine_vaso_amount" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f5 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f5="dopamine_vaso_rate" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f6 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f6="dopamine_vaso_amount" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f7 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f7="epinephrine_vaso_rate" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f8 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f8="epinephrine_vaso_amount" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f9 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f9="milrinone_vaso_rate" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f10 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f10="milrinone_vaso_amount" 
where f10 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f11 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f11="norepinephrine_vaso_rate" 
where f11 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f12 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f12="norepinephrine_vaso_amount" 
where f12 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f13 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f13="norepinephrine_equivalent_dose" 
where f13 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f14 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f14="phenylephrine_vaso_rate" 
where f14 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f15 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f15="phenylephrine_vaso_amount" 
where f15 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f16 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f16="vasopressin_vaso_rate" 
where f16 is null;
#################################################
ALTER TABLE `your-project-id1234.M60_GVASO.TabA5`
ADD column  f17 STRING ;
update `your-project-id1234.M60_GVASO.TabA5`
set f17="vasopressin_vaso_amount" 
where f17 is null;
#################################################

create table `your-project-id1234.M60_GVASO.TabA6`   as
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f1 as feature ,   CAST(administrationID AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`  
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f2 as feature , CAST(administration_Status AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`  
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f3 as feature , CAST(dobutamine_vaso_rate AS STRING) AS  value
from  `your-project-id1234.M60_GVASO.TabA5`  
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f4 as feature , CAST(dobutamine_vaso_amount AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f5 as feature , CAST(dopamine_vaso_rate AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f6 as feature , CAST(dopamine_vaso_amount AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f7 as feature , CAST(epinephrine_vaso_rate AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f8 as feature , CAST(epinephrine_vaso_amount AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f9 as feature , CAST(milrinone_vaso_rate AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f10 as feature , CAST(milrinone_vaso_amount AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f11 as feature , CAST(norepinephrine_vaso_rate AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f12 as feature , CAST(norepinephrine_vaso_amount AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f13 as feature , CAST(norepinephrine_equivalent_dose AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f14 as feature , CAST(phenylephrine_vaso_rate AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f15 as feature , CAST(phenylephrine_vaso_amount AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f16 as feature , CAST(vasopressin_vaso_rate AS STRING) AS value
from  `your-project-id1234.M60_GVASO.TabA5`
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f17 as feature , CAST(vasopressin_vaso_amount AS STRING) AS  value
from  `your-project-id1234.M60_GVASO.TabA5`;
#################################################
alter table `your-project-id1234.M60_GVASO.TabA6`
add column num INT64;
update`your-project-id1234.M60_GVASO.TabA6`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M60_GVASO.TabA7`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M60_GVASO.TabA6`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M60_GVASO.TabA6`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;




ALTER TABLE `your-project-id1234.M60_GVASO.TabA7`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M60_GVASO.TabA7`
SET Activity_Value_ID = concat("gvaso",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M60_GVASO.TabA8`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M60_GVASO.TabA7`  ;

CREATE TABLE `your-project-id1234.M60_GVASO.TabA9`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M60_GVASO.TabA7`  ;



ALTER TABLE `your-project-id1234.M60_GVASO.TabA9`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M60_GVASO.TabA9`
SET Activity_Synonym = "GVASO"
WHERE Activity_Synonym is null ;

ALTER TABLE `your-project-id1234.M60_GVASO.TabA9`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M60_GVASO.TabA9`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M60_GVASO.TabA10`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M60_GVASO.TabA9`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M60_GVASO.TabA9`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;



CREATE TABLE `your-project-id1234.M60_GVASO.TabA11`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M60_GVASO.TabA10`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M60_GVASO.TabA12`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M60_GVASO.TabA11`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M60_GVASO.TabA13`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M60_GVASO.TabA8`   as a
LEFT JOIN `your-project-id1234.M60_GVASO.TabA12`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M60_GVASO.TabA14`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M60_GVASO.TabA10`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M60_GVASO.TabA15`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M60_GVASO.TabA9`  )    ;



CREATE TABLE `your-project-id1234.M60_GVASO.TabA16`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M60_GVASO.TabA11`    as a
LEFT JOIN   `your-project-id1234.M60_GVASO.TabA12`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
