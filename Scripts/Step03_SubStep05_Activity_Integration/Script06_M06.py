import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''


create schema `your-project-id1234.M06_ACU` ;



create table `your-project-id1234.M06_ACU.TabA1` as
SELECT * FROM `your-project-id1234.S_Transfers.1_Transfer`
where eventtype="admit";


update `your-project-id1234.M06_ACU.TabA1`
set hadm_id=0
where hadm_id is null;




create table `your-project-id1234.M06_ACU.Temp`   as
Select * from `your-project-id1234.M06_ACU.TabA1`  ;

ALTER TABLE `your-project-id1234.M06_ACU.Temp`
ADD column  c1 STRING ;

UPDATE `your-project-id1234.M06_ACU.Temp`
SET c1 ="A"
WHERE c1 is null ;

create table `your-project-id1234.M06_ACU.TabA2`   as
Select
subject_id, hadm_id, transfer_id, eventtype, careunit, intime, outtime, Row_number() over (partition by c1 order by subject_id) as careUnit_Adm_ID
from `your-project-id1234.M06_ACU.Temp`  ;

drop table `your-project-id1234.M06_ACU.Temp`  ;







alter table `your-project-id1234.M06_ACU.TabA2` 
add column Activity string;

alter table `your-project-id1234.M06_ACU.TabA2` 
add column Activity_Synonym string;

update  `your-project-id1234.M06_ACU.TabA2` 
set Activity="Admission_to_Careunit"
where Activity is null;

update  `your-project-id1234.M06_ACU.TabA2` 
set Activity_Synonym="ACU"
where Activity_Synonym is null;

########################################################################

alter table `your-project-id1234.M06_ACU.TabA2` 
add column feature1 string;

alter table `your-project-id1234.M06_ACU.TabA2` 
add column feature2 string;

update  `your-project-id1234.M06_ACU.TabA2` 
set feature1="in_to"
where feature1 is null;

update  `your-project-id1234.M06_ACU.TabA2` 
set feature2="out_of"
where feature2 is null;





create table `your-project-id1234.M06_ACU.TabA3` as

select distinct * from(

SELECT subject_id, hadm_id ,Activity,Activity_Synonym, intime as Timestamps, 
feature1 as careUnit_Adm_status, careunit, careUnit_Adm_ID
FROM `your-project-id1234.M06_ACU.TabA2` 
union all
SELECT subject_id, hadm_id ,Activity,Activity_Synonym,  outtime as Timestamps,
feature2 as careUnit_Adm_status, careunit, careUnit_Adm_ID
FROM `your-project-id1234.M06_ACU.TabA2`)
;






create table `your-project-id1234.M06_ACU.TabA4` as
SELECT 
subject_id, hadm_id, Timestamps, Activity, Activity_Synonym
FROM `your-project-id1234.M06_ACU.TabA3`  ;






create table `your-project-id1234.M06_ACU.Temp`   as
Select * from `your-project-id1234.M06_ACU.TabA4`  ;

ALTER TABLE `your-project-id1234.M06_ACU.Temp`
ADD column  c1 STRING ;

UPDATE `your-project-id1234.M06_ACU.Temp`
SET c1 ="A"
WHERE c1 is null ;

create table `your-project-id1234.M06_ACU.Temp2`   as
Select
Row_number() over (partition by c1 order by subject_id, hadm_id, Timestamps, Activity, Activity_Synonym) as NewID, subject_id, hadm_id, Timestamps, Activity, Activity_Synonym
from `your-project-id1234.M06_ACU.Temp`  ;

ALTER TABLE `your-project-id1234.M06_ACU.Temp2`
ADD column  eventID STRING ;

UPDATE `your-project-id1234.M06_ACU.Temp2`
SET eventID = concat("aic",NewID)
WHERE eventID is null ;

create table `your-project-id1234.M06_ACU.TabA5`   as
Select
subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, eventID as Activity_Value_ID
from `your-project-id1234.M06_ACU.Temp2`  ;

drop table `your-project-id1234.M06_ACU.Temp`  ;
drop table `your-project-id1234.M06_ACU.Temp2`  ;











CREATE TABLE  `your-project-id1234.M06_ACU.TabA6`   AS
SELECT * FROM (
SELECT
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.careUnit_Adm_status , b.careunit , b.careUnit_Adm_ID,
From `your-project-id1234.M06_ACU.TabA5`   as a
LEFT JOIN `your-project-id1234.M06_ACU.TabA3`   as b
ON
a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id AND a.Timestamps=b.Timestamps AND a.Activity=b.Activity AND a.Activity_Synonym=b.Activity_Synonym )
;







CREATE TABLE  `your-project-id1234.M06_ACU.TabA7`   AS
SELECT 
Activity_Value_ID, Activity, 

careUnit_Adm_status as featureName1, 
careunit as featureName2,  
CAST(careUnit_Adm_ID AS STRING) AS featureName3,

careUnit_Adm_status as featureValue1, 
careunit as featureValue2 ,
CAST(careUnit_Adm_ID AS STRING) AS featureValue3

FROM `your-project-id1234.M06_ACU.TabA6` ;



update  `your-project-id1234.M06_ACU.TabA7` 
set featureName1="careunit_adm_status"
where featureName1 is not null;

update  `your-project-id1234.M06_ACU.TabA7` 
set featureName2="careunit"
where featureName2 is not null;

update  `your-project-id1234.M06_ACU.TabA7` 
set featureName3="careunit_adm_ID"
where featureName3 is not null;






CREATE TABLE  `your-project-id1234.M06_ACU.TabA8`   AS
SELECT 
Activity_Value_ID, Activity,
featureName1 as featureName,
featureValue1 as featureValue
FROM `your-project-id1234.M06_ACU.TabA7` 

union all

SELECT 
Activity_Value_ID, Activity,
featureName2 as featureName,
featureValue2 as featureValue
FROM `your-project-id1234.M06_ACU.TabA7` 


union all

SELECT 
Activity_Value_ID, Activity,
featureName3 as featureName,
featureValue3 as featureValue
FROM `your-project-id1234.M06_ACU.TabA7`;







ALTER TABLE `your-project-id1234.M06_ACU.TabA8`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M06_ACU.TabA8`
SET Activity_Synonym = "ACU"
WHERE Activity_Synonym is null ;



ALTER TABLE `your-project-id1234.M06_ACU.TabA8`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M06_ACU.TabA8`
SET num = 1
WHERE num is null ;







CREATE TABLE `your-project-id1234.M06_ACU.TabA9`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M06_ACU.TabA8`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M06_ACU.TabA8`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;






CREATE TABLE `your-project-id1234.M06_ACU.TabA10`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M06_ACU.TabA9`
where RankN is not null
order by Activity_Value_ID;





CREATE TABLE `your-project-id1234.M06_ACU.TabA11`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M06_ACU.TabA10`
GROUP BY Activity_Value_ID;






CREATE TABLE `your-project-id1234.M06_ACU.TabA12`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M06_ACU.TabA5`   as a
LEFT JOIN `your-project-id1234.M06_ACU.TabA11`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M06_ACU.TabA13`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M06_ACU.TabA9`
where RankN is not null)    ;


CREATE TABLE `your-project-id1234.M06_ACU.TabA14`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M06_ACU.TabA8`  )    ;



CREATE TABLE `your-project-id1234.M06_ACU.TabA15`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M06_ACU.TabA10`    as a
LEFT JOIN   `your-project-id1234.M06_ACU.TabA11`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;





'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
