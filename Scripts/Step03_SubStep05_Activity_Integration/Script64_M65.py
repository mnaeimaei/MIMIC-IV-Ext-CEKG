import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M65_CBC` ;
create table `your-project-id1234.M65_CBC.TabA1` as
SELECT * FROM `your-project-id1234.S_Derived1.complete_blood_count` ;



create table `your-project-id1234.M65_CBC.TabA2` as
SELECT * FROM  `your-project-id1234.M65_CBC.TabA1` ;
update  `your-project-id1234.M65_CBC.TabA2`
set hadm_id = 0
where hadm_id is null;



ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M65_CBC.TabA2`
SET Activity_Synonym ="CBC"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M65_CBC.TabA2`
SET Activity ="complete_blood_count"
WHERE Activity is null ;





ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  f1 STRING ;
update `your-project-id1234.M65_CBC.TabA2`
set f1="specimen_id" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  f2 STRING ;
update `your-project-id1234.M65_CBC.TabA2`
set f2="hematocrit" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  f3 STRING ;
update `your-project-id1234.M65_CBC.TabA2`
set f3="hemoglobin" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  f4 STRING ;
update `your-project-id1234.M65_CBC.TabA2`
set f4="mch" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  f5 STRING ;
update `your-project-id1234.M65_CBC.TabA2`
set f5="mchc" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  f6 STRING ;
update `your-project-id1234.M65_CBC.TabA2`
set f6="mcv" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  f7 STRING ;
update `your-project-id1234.M65_CBC.TabA2`
set f7="platelet" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  f8 STRING ;
update `your-project-id1234.M65_CBC.TabA2`
set f8="rbc" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  f9 STRING ;
update `your-project-id1234.M65_CBC.TabA2`
set f9="rdw" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  f10 STRING ;
update `your-project-id1234.M65_CBC.TabA2`
set f10="rdwsd" 
where f10 is null;
#################################################
ALTER TABLE `your-project-id1234.M65_CBC.TabA2`
ADD column  f11 STRING ;
update `your-project-id1234.M65_CBC.TabA2`
set f11="wbc" 
where f11 is null;
################################################################
create table `your-project-id1234.M65_CBC.TabA3`   as
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(specimen_id AS STRING) as value
from  `your-project-id1234.M65_CBC.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(hematocrit AS STRING) as value
from  `your-project-id1234.M65_CBC.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(hemoglobin AS STRING) as value
from  `your-project-id1234.M65_CBC.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(mch AS STRING) as value
from  `your-project-id1234.M65_CBC.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(mchc AS STRING) as value
from  `your-project-id1234.M65_CBC.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(mcv AS STRING) as value
from  `your-project-id1234.M65_CBC.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(platelet AS STRING) as value
from  `your-project-id1234.M65_CBC.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(rbc AS STRING) as  value
from  `your-project-id1234.M65_CBC.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(rdw AS STRING) as value
from  `your-project-id1234.M65_CBC.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(rdwsd AS STRING) as value
from  `your-project-id1234.M65_CBC.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f11 as feature , CAST(wbc AS STRING) as value
from  `your-project-id1234.M65_CBC.TabA2`;
#################################################
alter table `your-project-id1234.M65_CBC.TabA3`
add column num INT64;
update`your-project-id1234.M65_CBC.TabA3`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M65_CBC.TabA4`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M65_CBC.TabA3`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M65_CBC.TabA3`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;






ALTER TABLE `your-project-id1234.M65_CBC.TabA4`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M65_CBC.TabA4`
SET Activity_Value_ID = concat("cbc",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M65_CBC.TabA5`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M65_CBC.TabA4`  ;

CREATE TABLE `your-project-id1234.M65_CBC.TabA6`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M65_CBC.TabA4`  ;



ALTER TABLE `your-project-id1234.M65_CBC.TabA6`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M65_CBC.TabA6`
SET Activity_Synonym = "CBC"
WHERE Activity_Synonym is null ;


ALTER TABLE `your-project-id1234.M65_CBC.TabA6`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M65_CBC.TabA6`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M65_CBC.TabA7`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M65_CBC.TabA6`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M65_CBC.TabA6`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M65_CBC.TabA8`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M65_CBC.TabA7`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M65_CBC.TabA9`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M65_CBC.TabA8`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M65_CBC.TabA10`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M65_CBC.TabA5`   as a
LEFT JOIN `your-project-id1234.M65_CBC.TabA9`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M65_CBC.TabA11`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M65_CBC.TabA7`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M65_CBC.TabA12`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M65_CBC.TabA6`  )    ;



CREATE TABLE `your-project-id1234.M65_CBC.TabA13`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M65_CBC.TabA8`    as a
LEFT JOIN   `your-project-id1234.M65_CBC.TabA9`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;






'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
