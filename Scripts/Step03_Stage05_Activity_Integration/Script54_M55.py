import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M55_UCR` ;
create table `your-project-id1234.M55_UCR.TabA1` as
select * from  `your-project-id1234.N04_CHB.xLab31`  ;



create table `your-project-id1234.M55_UCR.TabA2` as
SELECT 
subject_id, hadm_id, Timestp, fluid as Activity, fluid as Activity_Syn,
fluidLabelShort as feature,  fluidLabelValue as value
FROM `your-project-id1234.M55_UCR.TabA1` ;
update  `your-project-id1234.M55_UCR.TabA2`
set Activity_Syn="UCR"
where Activity_Syn is not null;
update  `your-project-id1234.M55_UCR.TabA2`
set Activity="urine_sample_random_Culture_test"
where Activity is not null;



create table `your-project-id1234.M55_UCR.TabA3`   as
Select
subject_id, hadm_id, Timestp as Timestamps, Activity, Activity_Syn as Activity_Synonym, feature, value
from `your-project-id1234.M55_UCR.TabA2`  ;

alter table `your-project-id1234.M55_UCR.TabA3`
add column num INT64;
update`your-project-id1234.M55_UCR.TabA3`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M55_UCR.TabA4`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M55_UCR.TabA3`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M55_UCR.TabA3`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;





ALTER TABLE `your-project-id1234.M55_UCR.TabA4`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M55_UCR.TabA4`
SET Activity_Value_ID = concat("ucr",RankN)
WHERE Activity_Value_ID is null ;





CREATE TABLE `your-project-id1234.M55_UCR.TabA5`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M55_UCR.TabA4`  ;

CREATE TABLE `your-project-id1234.M55_UCR.TabA6`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M55_UCR.TabA4`  ;




ALTER TABLE `your-project-id1234.M55_UCR.TabA6`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M55_UCR.TabA6`
SET Activity_Synonym = "UCR"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M55_UCR.TabA6`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M55_UCR.TabA6`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M55_UCR.TabA7`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M55_UCR.TabA6`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M55_UCR.TabA6`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;



CREATE TABLE `your-project-id1234.M55_UCR.TabA8`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M55_UCR.TabA7`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M55_UCR.TabA9`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M55_UCR.TabA8`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M55_UCR.TabA10`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M55_UCR.TabA5`   as a
LEFT JOIN `your-project-id1234.M55_UCR.TabA9`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M55_UCR.TabA11`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M55_UCR.TabA7`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M55_UCR.TabA12`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M55_UCR.TabA6`  )    ;



CREATE TABLE `your-project-id1234.M55_UCR.TabA13`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M55_UCR.TabA8`    as a
LEFT JOIN   `your-project-id1234.M55_UCR.TabA9`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;





'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
