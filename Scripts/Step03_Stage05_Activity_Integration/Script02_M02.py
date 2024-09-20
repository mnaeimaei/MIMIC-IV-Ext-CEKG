import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''


create schema `mimic-four-ml4219.M02_HA` ;


create table `mimic-four-ml4219.M02_HA.TabA1` as
SELECT * FROM `mimic-four-ml4219.S_Admission.2_Admissions` ;



create table `mimic-four-ml4219.M02_HA.TabA2` as
SELECT subject_id, hadm_id, admittime as Timestamp, admission_location, admission_type
FROM `mimic-four-ml4219.M02_HA.TabA1`
where admittime is not null ;

alter table `mimic-four-ml4219.M02_HA.TabA2`
add column Activity string;

alter table `mimic-four-ml4219.M02_HA.TabA2` 
add column Activity_Synonym string;

update  `mimic-four-ml4219.M02_HA.TabA2` 
set Activity="Hospital_Admission"
where Activity is null;

update  `mimic-four-ml4219.M02_HA.TabA2` 
set Activity_Synonym="HA"
where Activity_Synonym is null;

####################################################3

update `mimic-four-ml4219.M02_HA.TabA2` 
set admission_location="NULL"
where admission_location is null;


update `mimic-four-ml4219.M02_HA.TabA2` 
set admission_type="NULL"
where admission_type is null;


############################################################################

create table `mimic-four-ml4219.M02_HA.TabA3` as
SELECT 
subject_id, hadm_id, Timestamp, Activity, Activity_Synonym
FROM `mimic-four-ml4219.M02_HA.TabA2`  ;

#############################################################################



create table `mimic-four-ml4219.M02_HA.Temp`   as
Select * from `mimic-four-ml4219.M02_HA.TabA3`  ;

ALTER TABLE `mimic-four-ml4219.M02_HA.Temp`
ADD column  c1 STRING ;

UPDATE `mimic-four-ml4219.M02_HA.Temp`
SET c1 ="A"
WHERE c1 is null ;

create table `mimic-four-ml4219.M02_HA.Temp2`   as
Select
Row_number() over (partition by c1 order by subject_id, hadm_id, Timestamp, Activity, Activity_Synonym) as NewID, subject_id, hadm_id, Timestamp, Activity, Activity_Synonym
from `mimic-four-ml4219.M02_HA.Temp`  ;

ALTER TABLE `mimic-four-ml4219.M02_HA.Temp2`
ADD column  eventID STRING ;

UPDATE `mimic-four-ml4219.M02_HA.Temp2`
SET eventID = concat("ha",NewID)
WHERE eventID is null ;

create table `mimic-four-ml4219.M02_HA.TabA4`   as
Select
subject_id, hadm_id, Timestamp, Activity, Activity_Synonym, eventID as Activity_Value_ID
from `mimic-four-ml4219.M02_HA.Temp2`  ;

drop table `mimic-four-ml4219.M02_HA.Temp`  ;
drop table `mimic-four-ml4219.M02_HA.Temp2`  ;



CREATE TABLE  `mimic-four-ml4219.M02_HA.TabA5`   AS
SELECT distinct * FROM (
SELECT
a.subject_id , a.hadm_id , a.Timestamp , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.admission_location , b.admission_type
From `mimic-four-ml4219.M02_HA.TabA4`   as a
LEFT JOIN `mimic-four-ml4219.M02_HA.TabA2`   as b
ON
a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id AND a.Timestamp=b.Timestamp AND a.Activity=b.Activity AND a.Activity_Synonym=b.Activity_Synonym )        ;



CREATE TABLE  `mimic-four-ml4219.M02_HA.TabA6`   AS
SELECT 
Activity_Value_ID, Activity, admission_location, admission_type
FROM `mimic-four-ml4219.M02_HA.TabA5` ;



alter table `mimic-four-ml4219.M02_HA.TabA6` 
add column featureName1 string;

alter table `mimic-four-ml4219.M02_HA.TabA6` 
add column featureName2 string;

######################################################


update `mimic-four-ml4219.M02_HA.TabA6` 
set featureName1="admission_location"
where featureName1 is null;


update `mimic-four-ml4219.M02_HA.TabA6` 
set featureName2="admission_type"
where featureName2 is null;



create table `mimic-four-ml4219.M02_HA.TabA7`   as
SELECT
Activity_Value_ID, Activity, featureName1 as featureName, admission_location as featureValue
FROM `mimic-four-ml4219.M02_HA.TabA6`
union all
SELECT
Activity_Value_ID, Activity, featureName2 as featureName, admission_type as featureValue
FROM `mimic-four-ml4219.M02_HA.TabA6`;



ALTER TABLE `mimic-four-ml4219.M02_HA.TabA7`
ADD column  Activity_Synonym STRING ;

UPDATE `mimic-four-ml4219.M02_HA.TabA7`
SET Activity_Synonym = "HA"
WHERE Activity_Synonym is null ;


ALTER TABLE `mimic-four-ml4219.M02_HA.TabA7`
ADD column  num INT64 ;

UPDATE `mimic-four-ml4219.M02_HA.TabA7`
SET num = 1
WHERE num is null ;



CREATE TABLE `mimic-four-ml4219.M02_HA.TabA8`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `mimic-four-ml4219.M02_HA.TabA7`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `mimic-four-ml4219.M02_HA.TabA7`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;



CREATE TABLE `mimic-four-ml4219.M02_HA.TabA9`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `mimic-four-ml4219.M02_HA.TabA8`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `mimic-four-ml4219.M02_HA.TabA10`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `mimic-four-ml4219.M02_HA.TabA9`
GROUP BY Activity_Value_ID;



CREATE TABLE `mimic-four-ml4219.M02_HA.TabA11`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamp as Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `mimic-four-ml4219.M02_HA.TabA4`   as a
LEFT JOIN `mimic-four-ml4219.M02_HA.TabA10`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `mimic-four-ml4219.M02_HA.TabA12`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `mimic-four-ml4219.M02_HA.TabA8`
where RankN is not null)    ;


CREATE TABLE `mimic-four-ml4219.M02_HA.TabA13`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `mimic-four-ml4219.M02_HA.TabA7`  )    ;



CREATE TABLE `mimic-four-ml4219.M02_HA.TabA14`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `mimic-four-ml4219.M02_HA.TabA9`    as a
LEFT JOIN   `mimic-four-ml4219.M02_HA.TabA10`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
