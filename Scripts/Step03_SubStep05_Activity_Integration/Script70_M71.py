import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M71_ILI` ;
create table `your-project-id1234.M71_ILI.TabA1` as
SELECT * FROM `your-project-id1234.S_Derived1.invasive_line` ;



create table `your-project-id1234.M71_ILI.Temp`   as
Select * from `your-project-id1234.M71_ILI.TabA1`  ;

ALTER TABLE `your-project-id1234.M71_ILI.Temp`
ADD column  c1 STRING ;

UPDATE `your-project-id1234.M71_ILI.Temp`
SET c1 ="A"
WHERE c1 is null ;

create table `your-project-id1234.M71_ILI.TabA2`   as
Select
stay_id, line_type, line_site, starttime, endtime, Row_number() over (partition by c1 order by stay_id) as Invasive_Line_ID,
from `your-project-id1234.M71_ILI.Temp`  ;

drop table `your-project-id1234.M71_ILI.Temp`  ;



alter table `your-project-id1234.M71_ILI.TabA2`
add column ss string;

alter table `your-project-id1234.M71_ILI.TabA2`
add column ee string;

update `your-project-id1234.M71_ILI.TabA2`
set ss="start"
where ss is null and starttime is not null;

update `your-project-id1234.M71_ILI.TabA2`
set ee="end"
where ee is null and endtime is not null;




CREATE TABLE  `your-project-id1234.M71_ILI.TabA3`  as

SELECT stay_id , starttime as Timestamps , line_type , line_site ,  Invasive_Line_ID , ss as Invasive_Line_Status
FROM `your-project-id1234.M71_ILI.TabA2`

union all

SELECT stay_id , endtime as Timestamps , line_type , line_site , Invasive_Line_ID , ee  as Invasive_Line_Status
FROM `your-project-id1234.M71_ILI.TabA2`  ;



CREATE TABLE  `your-project-id1234.M71_ILI.TabA4`   AS
SELECT distinct * FROM (
SELECT
b.subject_id , b.hadm_id, a.stay_id , a.Timestamps , a.line_type , a.line_site , a.Invasive_Line_ID , a.Invasive_Line_Status,

From `your-project-id1234.M71_ILI.TabA3`   as a
LEFT JOIN `your-project-id1234.R_TimeC.SHI`   as b
ON
a.stay_id=b.stay_id
AND  a.Timestamps>=b.min AND a.Timestamps<=b.max
)
;



CREATE TABLE  `your-project-id1234.M71_ILI.TabA5`   AS
SELECT distinct   
subject_id, hadm_id, Timestamps, line_type, line_site, Invasive_Line_ID, Invasive_Line_Status  
From `your-project-id1234.M71_ILI.TabA4` ;



ALTER TABLE `your-project-id1234.M71_ILI.TabA5`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M71_ILI.TabA5`
SET Activity_Synonym ="ILI"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M71_ILI.TabA5`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M71_ILI.TabA5`
SET Activity ="Invasive_line_insertion"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M71_ILI.TabA5`
ADD column  f1 STRING ;
update `your-project-id1234.M71_ILI.TabA5`
set f1="line_type" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M71_ILI.TabA5`
ADD column  f2 STRING ;
update `your-project-id1234.M71_ILI.TabA5`
set f2="line_site" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M71_ILI.TabA5`
ADD column  f3 STRING ;
update `your-project-id1234.M71_ILI.TabA5`
set f3="Invasive_Line_ID" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M71_ILI.TabA5`
ADD column  f4 STRING ;
update `your-project-id1234.M71_ILI.TabA5`
set f4="Invasive_Line_Status" 
where f4 is null;
################################################################

create table `your-project-id1234.M71_ILI.TabA6`   as
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(line_type AS STRING) as value
from  `your-project-id1234.M71_ILI.TabA5`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(line_site AS STRING) as value
from  `your-project-id1234.M71_ILI.TabA5`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(Invasive_Line_ID AS STRING) as value
from  `your-project-id1234.M71_ILI.TabA5`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(Invasive_Line_Status AS STRING) as value
from  `your-project-id1234.M71_ILI.TabA5` ;
#################################################
alter table `your-project-id1234.M71_ILI.TabA6`
add column num INT64;
update`your-project-id1234.M71_ILI.TabA6`
set num=1
where num is null;




CREATE TABLE `your-project-id1234.M71_ILI.TabA7`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M71_ILI.TabA6`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M71_ILI.TabA6`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;





ALTER TABLE `your-project-id1234.M71_ILI.TabA7`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M71_ILI.TabA7`
SET Activity_Value_ID = concat("ili",RankN)
WHERE Activity_Value_ID is null ;



CREATE TABLE `your-project-id1234.M71_ILI.TabA8`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M71_ILI.TabA7`  ;

CREATE TABLE `your-project-id1234.M71_ILI.TabA9`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M71_ILI.TabA7`  ;



ALTER TABLE `your-project-id1234.M71_ILI.TabA9`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M71_ILI.TabA9`
SET Activity_Synonym = "ILI"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M71_ILI.TabA9`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M71_ILI.TabA9`
SET num = 1
WHERE num is null ;




CREATE TABLE `your-project-id1234.M71_ILI.TabA10`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M71_ILI.TabA9`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M71_ILI.TabA9`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M71_ILI.TabA11`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M71_ILI.TabA10`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M71_ILI.TabA12`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M71_ILI.TabA11`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M71_ILI.TabA13`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M71_ILI.TabA8`   as a
LEFT JOIN `your-project-id1234.M71_ILI.TabA12`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M71_ILI.TabA14`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M71_ILI.TabA10`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M71_ILI.TabA15`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M71_ILI.TabA9`  )    ;



CREATE TABLE `your-project-id1234.M71_ILI.TabA16`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M71_ILI.TabA11`    as a
LEFT JOIN   `your-project-id1234.M71_ILI.TabA12`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;





'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
