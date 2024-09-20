import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''


create schema `your-project-id1234.M90_ANT` ;
create table `your-project-id1234.M90_ANT.TabA1` as
SELECT * FROM `your-project-id1234.N16_sofa.antibiotic` ;



create table `your-project-id1234.M90_ANT.Temp`   as
Select * from `your-project-id1234.M90_ANT.TabA1`  ;

ALTER TABLE `your-project-id1234.M90_ANT.Temp`
ADD column  c1 STRING ;

UPDATE `your-project-id1234.M90_ANT.Temp`
SET c1 ="A"
WHERE c1 is null ;

create table `your-project-id1234.M90_ANT.TabA2`   as
Select
subject_id, hadm_id, stay_id, antibiotic, route, starttime, stoptime, Row_number() over (partition by c1 order by subject_id) as antibioticID,
from `your-project-id1234.M90_ANT.Temp`  ;

drop table `your-project-id1234.M90_ANT.Temp`  ;



alter table `your-project-id1234.M90_ANT.TabA2`
add column ss string;

alter table `your-project-id1234.M90_ANT.TabA2`
add column ee string;

update `your-project-id1234.M90_ANT.TabA2`
set ss="start"
where ss is null and starttime is not null;

update `your-project-id1234.M90_ANT.TabA2`
set ee="end"
where ee is null and stoptime is not null;





CREATE TABLE  `your-project-id1234.M90_ANT.TabA3`  as
SELECT subject_id, hadm_id, stay_id,
starttime as Timestamps , 
antibioticID , 
ss as Invasive_Line_Status , 
antibiotic, route
FROM `your-project-id1234.M90_ANT.TabA2`  
union all
SELECT subject_id, hadm_id, stay_id,
stoptime as Timestamps , 
antibioticID , 
ee  as Invasive_Line_Status , 
antibiotic, route
FROM `your-project-id1234.M90_ANT.TabA2`  ;



CREATE TABLE  `your-project-id1234.M90_ANT.TabA4`  as
select * from  `your-project-id1234.M90_ANT.TabA3`  
where Timestamps is not null;



CREATE TABLE  `your-project-id1234.M90_ANT.TabA5`   AS
SELECT distinct * FROM (
SELECT
a.subject_id , a.hadm_id , a.stay_id , a.Timestamps , a.antibioticID , a.Invasive_Line_Status , a.antibiotic , a.route,
b.stay_id as STI,
From `your-project-id1234.M90_ANT.TabA4`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SHI`   as b
ON
a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id
AND  a.Timestamps>=b.min AND a.Timestamps<=b.max
)
;



update `your-project-id1234.M90_ANT.TabA5`
set stay_id = STI
where stay_id is null and STI is not null;





CREATE TABLE  `your-project-id1234.M90_ANT.TabA6`   AS
SELECT distinct
subject_id, hadm_id, stay_id, Timestamps, antibioticID, Invasive_Line_Status, antibiotic, route
FROM `your-project-id1234.M90_ANT.TabA5` ;



CREATE TABLE  `your-project-id1234.M90_ANT.TabA7`   AS  
SELECT distinct   
subject_id, hadm_id, Timestamps, antibioticID, Invasive_Line_Status, antibiotic, route  
From `your-project-id1234.M90_ANT.TabA6` ;



ALTER TABLE `your-project-id1234.M90_ANT.TabA7`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M90_ANT.TabA7`
SET Activity_Synonym ="ANT"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M90_ANT.TabA7`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M90_ANT.TabA7`
SET Activity ="Antibiotic_Administration"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M90_ANT.TabA7`
ADD column  f1 STRING ;
update `your-project-id1234.M90_ANT.TabA7`
set f1="antibioticID" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M90_ANT.TabA7`
ADD column  f2 STRING ;
update `your-project-id1234.M90_ANT.TabA7`
set f2="Invasive_Line_Status" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M90_ANT.TabA7`
ADD column  f3 STRING ;
update `your-project-id1234.M90_ANT.TabA7`
set f3="antibiotic" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M90_ANT.TabA7`
ADD column  f4 STRING ;
update `your-project-id1234.M90_ANT.TabA7`
set f4="route" 
where f4 is null;
################################################################

create table `your-project-id1234.M90_ANT.TabA8`   as
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(antibioticID AS STRING) as value
from  `your-project-id1234.M90_ANT.TabA7`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(Invasive_Line_Status AS STRING) as value
from  `your-project-id1234.M90_ANT.TabA7`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(antibiotic AS STRING) as value
from  `your-project-id1234.M90_ANT.TabA7`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(route AS STRING) as value
from  `your-project-id1234.M90_ANT.TabA7` ;
#################################################
alter table `your-project-id1234.M90_ANT.TabA8`
add column num INT64;
update`your-project-id1234.M90_ANT.TabA8`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M90_ANT.TabA9`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M90_ANT.TabA8`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M90_ANT.TabA8`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;






ALTER TABLE `your-project-id1234.M90_ANT.TabA9`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M90_ANT.TabA9`
SET Activity_Value_ID = concat("ant",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M90_ANT.TabA10`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M90_ANT.TabA9`  ;

CREATE TABLE `your-project-id1234.M90_ANT.TabA11`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M90_ANT.TabA9`  ;



ALTER TABLE `your-project-id1234.M90_ANT.TabA11`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M90_ANT.TabA11`
SET Activity_Synonym = "ANT"
WHERE Activity_Synonym is null ;


ALTER TABLE `your-project-id1234.M90_ANT.TabA11`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M90_ANT.TabA11`
SET num = 1
WHERE num is null ;




CREATE TABLE `your-project-id1234.M90_ANT.TabA12`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M90_ANT.TabA11`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M90_ANT.TabA11`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M90_ANT.TabA13`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M90_ANT.TabA12`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M90_ANT.TabA14`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M90_ANT.TabA13`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M90_ANT.TabA15`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M90_ANT.TabA10`   as a
LEFT JOIN `your-project-id1234.M90_ANT.TabA14`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M90_ANT.TabA16`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M90_ANT.TabA12`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M90_ANT.TabA17`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M90_ANT.TabA11`  )    ;



CREATE TABLE `your-project-id1234.M90_ANT.TabA18`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M90_ANT.TabA13`    as a
LEFT JOIN   `your-project-id1234.M90_ANT.TabA14`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
