import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M10_MED` ;
create table `your-project-id1234.M10_MED.TabA1` as
SELECT * FROM `your-project-id1234.N01_NDC.zzzzt5` ;
create table `your-project-id1234.M10_MED.Tab_NDC` as
SELECT * FROM `your-project-id1234.N01_NDC.x3` ;



create table `your-project-id1234.M10_MED.TabA2` as
SELECT 
eventID, subject_id, hadm_id, starttime, stoptime,charttime,  event_txt, Ndc
FROM `your-project-id1234.M10_MED.TabA1` ;



update `your-project-id1234.M10_MED.TabA2`
set stoptime=null
where starttime is not null and  stoptime is not null and stoptime<starttime;
update `your-project-id1234.M10_MED.TabA2`
set charttime=stoptime
where charttime is not null and  stoptime is not null and stoptime<charttime;
update `your-project-id1234.M10_MED.TabA2`
set charttime=stoptime
where charttime is null and  stoptime is not null;
update `your-project-id1234.M10_MED.TabA2`
set starttime=null
where starttime is not null and  charttime is not null and charttime<starttime;



create table `your-project-id1234.M10_MED.TabA3` as
SELECT 
subject_id, hadm_id, starttime, charttime as stoptime, event_txt as e1, event_txt as e2, event_txt, Ndc
FROM `your-project-id1234.M10_MED.TabA2` ;
update `your-project-id1234.M10_MED.TabA3`
set e1="Start"
where event_txt is not null or  event_txt is null;
update `your-project-id1234.M10_MED.TabA3`
set e2="Stop"
where event_txt is not null  or  event_txt is null;




create table `your-project-id1234.M10_MED.Temp`   as
Select * from `your-project-id1234.M10_MED.TabA3`  ;

ALTER TABLE `your-project-id1234.M10_MED.Temp`
ADD column  c1 STRING ;

UPDATE `your-project-id1234.M10_MED.Temp`
SET c1 ="A"
WHERE c1 is null ;

create table `your-project-id1234.M10_MED.TabA4`   as
Select
subject_id, hadm_id, starttime, stoptime, e1, e2, event_txt, Ndc, Row_number() over (partition by c1 order by subject_id) as Medication_ID
from `your-project-id1234.M10_MED.Temp`  ;

drop table `your-project-id1234.M10_MED.Temp`  ;



create table `your-project-id1234.M10_MED.TabA5` as
select distinct * from
(SELECT subject_id, hadm_id,  starttime as Timestamps,  NDC , e1 as Medication_Status, Medication_ID 
FROM `your-project-id1234.M10_MED.TabA4` where starttime is not null
union all 
SELECT subject_id, hadm_id, stoptime as Timestamps,  NDC , e2 as Medication_Status, Medication_ID 
FROM `your-project-id1234.M10_MED.TabA4` where stoptime is not null );



create table `your-project-id1234.M10_MED.TabA6` as
SELECT * FROM `your-project-id1234.M10_MED.TabA5` 
where NDC is not null;



ALTER   TABLE `your-project-id1234.M10_MED.TabA6`
ADD    COLUMN  Activity STRING ;
UPDATE `your-project-id1234.M10_MED.TabA6`
SET  Activity  = "Medication"
WHERE Activity is null ;
############################################################
ALTER   TABLE `your-project-id1234.M10_MED.TabA6`
ADD    COLUMN  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M10_MED.TabA6`
SET  Activity_Synonym  = "MED"
WHERE Activity_Synonym is null ;



ALTER TABLE `your-project-id1234.M10_MED.TabA6`
ADD column  f1 STRING ;
update `your-project-id1234.M10_MED.TabA6`
set f1="NDC" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M10_MED.TabA6`
ADD column  f2 STRING ;
update `your-project-id1234.M10_MED.TabA6`
set f2="Medication_Status" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M10_MED.TabA6`
ADD column  f3 STRING ;
update `your-project-id1234.M10_MED.TabA6`
set f3="Medication_ID" 
where f3 is null;
#################################################

create table `your-project-id1234.M10_MED.TabA7`   as
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f1 as feature , NDC as value
from  `your-project-id1234.M10_MED.TabA6`  
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f2 as feature , Medication_Status as value
from  `your-project-id1234.M10_MED.TabA6`  
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f3 as feature ,   CAST(Medication_ID AS STRING) AS value
from  `your-project-id1234.M10_MED.TabA6`;
#################################################
alter table `your-project-id1234.M10_MED.TabA7`
add column num INT64;
update`your-project-id1234.M10_MED.TabA7`
set num=1
where num is null;




CREATE TABLE `your-project-id1234.M10_MED.TabA8`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M10_MED.TabA7`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M10_MED.TabA7`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;






ALTER TABLE `your-project-id1234.M10_MED.TabA8`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M10_MED.TabA8`
SET Activity_Value_ID = concat("med",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M10_MED.TabA9`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234..M10_MED.TabA8`  ;

CREATE TABLE `your-project-id1234.M10_MED.TabA10`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234..M10_MED.TabA8`  ;



ALTER TABLE `your-project-id1234.M10_MED.TabA10`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M10_MED.TabA10`
SET Activity_Synonym = "MED"
WHERE Activity_Synonym is null ;

ALTER TABLE `your-project-id1234.M10_MED.TabA10`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M10_MED.TabA10`
SET num = 1
WHERE num is null ;




CREATE TABLE `your-project-id1234.M10_MED.TabA11`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M10_MED.TabA10`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M10_MED.TabA10`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;



CREATE TABLE `your-project-id1234.M10_MED.TabA12`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M10_MED.TabA11`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M10_MED.TabA13`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M10_MED.TabA12`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M10_MED.TabA14`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M10_MED.TabA9`   as a
LEFT JOIN `your-project-id1234.M10_MED.TabA13`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M10_MED.TabA15`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M10_MED.TabA11`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M10_MED.TabA16`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M10_MED.TabA10`  )    ;



CREATE TABLE `your-project-id1234.M10_MED.TabA17`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M10_MED.TabA12`    as a
LEFT JOIN   `your-project-id1234.M10_MED.TabA13`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
