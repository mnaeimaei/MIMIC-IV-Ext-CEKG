import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M18_TFO` ;
create table `your-project-id1234.M18_TFO.TabA1` as
SELECT * FROM `your-project-id1234.N02_POE.4_POE_Transfer` ;



create table `your-project-id1234.M18_TFO.TabA2` as
SELECT distinct 
subject_id, hadm_id, ordertime as Timestamps, 
field_name as Activity,
field_name as Activity_Syn,
field_value as Transfer_to,
order_type as type,
transaction_type as provider_action
FROM `your-project-id1234.M18_TFO.TabA1` ;





update   `your-project-id1234.M18_TFO.TabA2`
set Activity = "Transfer_Order"
where Activity is not null;
update   `your-project-id1234.M18_TFO.TabA2`
set Activity_Syn = "TFO"
where Activity_Syn is not null;



ALTER TABLE `your-project-id1234.M18_TFO.TabA2`
ADD column  f1 STRING ;
update `your-project-id1234.M18_TFO.TabA2`
set f1="Transfer_to" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M18_TFO.TabA2`
ADD column  f2 STRING ;
update `your-project-id1234.M18_TFO.TabA2`
set f2="type" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M18_TFO.TabA2`
ADD column  f3 STRING ;
update `your-project-id1234.M18_TFO.TabA2`
set f3="provider_action" 
where f3 is null;
#################################################

create table `your-project-id1234.M18_TFO.TabA3`   as
select subject_id, hadm_id, Timestamps, Activity, Activity_Syn   as   Activity_Synonym, f1 as feature , Transfer_to as value
from  `your-project-id1234.M18_TFO.TabA2`  
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Syn   as   Activity_Synonym, f2 as feature , type as value
from  `your-project-id1234.M18_TFO.TabA2`  
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Syn   as   Activity_Synonym, f3 as feature , provider_action as value
from  `your-project-id1234.M18_TFO.TabA2` ;
#################################################
alter table `your-project-id1234.M18_TFO.TabA3`
add column num INT64;
update`your-project-id1234.M18_TFO.TabA3`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M18_TFO.TabA4`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M18_TFO.TabA3`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M18_TFO.TabA3`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;






ALTER TABLE `your-project-id1234.M18_TFO.TabA4`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M18_TFO.TabA4`
SET Activity_Value_ID = concat("tfo",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M18_TFO.TabA5`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234..M18_TFO.TabA4`  ;

CREATE TABLE `your-project-id1234.M18_TFO.TabA6`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234..M18_TFO.TabA4`  ;



ALTER TABLE `your-project-id1234.M18_TFO.TabA6`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M18_TFO.TabA6`
SET Activity_Synonym = "TFO"
WHERE Activity_Synonym is null ;

ALTER TABLE `your-project-id1234.M18_TFO.TabA6`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M18_TFO.TabA6`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M18_TFO.TabA7`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M18_TFO.TabA6`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M18_TFO.TabA6`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M18_TFO.TabA8`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M18_TFO.TabA7`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M18_TFO.TabA9`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M18_TFO.TabA8`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M18_TFO.TabA10`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M18_TFO.TabA5`   as a
LEFT JOIN `your-project-id1234.M18_TFO.TabA9`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M18_TFO.TabA11`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M18_TFO.TabA7`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M18_TFO.TabA12`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M18_TFO.TabA6`  )    ;



CREATE TABLE `your-project-id1234.M18_TFO.TabA13`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M18_TFO.TabA8`    as a
LEFT JOIN   `your-project-id1234.M18_TFO.TabA9`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
