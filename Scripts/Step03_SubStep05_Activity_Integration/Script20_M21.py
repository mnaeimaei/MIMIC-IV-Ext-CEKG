import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M21_HMS` ;
create table `your-project-id1234.M21_HMS.TabA1` as
SELECT * FROM `your-project-id1234.N03_OMR.02_Height` ;
create table `your-project-id1234.M21_HMS.TabB1` as
SELECT * FROM `your-project-id1234.S_Derived1.height` ;
create table `your-project-id1234.M21_HMS.TabC1` as
SELECT * FROM `your-project-id1234.N12_Height.Height_Final` ;



create table `your-project-id1234.M21_HMS.TabA2` as
SELECT * FROM `your-project-id1234.M21_HMS.TabA1` ;
create table `your-project-id1234.M21_HMS.TabB2` as
SELECT * FROM `your-project-id1234.M21_HMS.TabB1` ;
create table `your-project-id1234.M21_HMS.TabC2` as
SELECT * FROM `your-project-id1234.M21_HMS.TabC1` ;



#############################################################
CREATE TABLE  `your-project-id1234.M21_HMS.TabA3`   AS
SELECT 
subject_id, hadm_id, Timestamps, result_value as Height
FROM `your-project-id1234.M21_HMS.TabA2` ;
#############################################################



CREATE TABLE  `your-project-id1234.M21_HMS.TabB3`   AS
SELECT distinct * FROM (
SELECT
a.subject_id , b.hadm_id ,  a.stay_id , a.charttime as Timestamps,  CAST(a.height AS STRING) AS Height
From `your-project-id1234.M21_HMS.TabB2`   as a
LEFT JOIN `your-project-id1234.R_TimeC.SHI`   as b
ON
a.stay_id=b.stay_id AND a.subject_id=b.subject_id 
AND  a.charttime>=b.min AND a.charttime<=b.max 
)    
; 




CREATE TABLE  `your-project-id1234.M21_HMS.TabC3`   AS
SELECT distinct * FROM (
SELECT
a.subject_id , b.hadm_id ,  a.stay_id , a.charttime as Timestamps,  CAST(a.height AS STRING) AS Height
From `your-project-id1234.M21_HMS.TabC2`   as a
LEFT JOIN `your-project-id1234.R_TimeC.SHI`   as b
ON
a.stay_id=b.stay_id AND a.subject_id=b.subject_id 
AND  a.charttime>=b.min AND a.charttime<=b.max 
)    
; 



CREATE TABLE  `your-project-id1234.M21_HMS.TabD1`   AS
SELECT subject_id,hadm_id, Timestamps, Height FROM `your-project-id1234.M21_HMS.TabA3` 
union distinct
SELECT subject_id,hadm_id, Timestamps, Height FROM `your-project-id1234.M21_HMS.TabB3` 
union distinct
SELECT subject_id,hadm_id, Timestamps, Height FROM `your-project-id1234.M21_HMS.TabC3` ;



ALTER TABLE `your-project-id1234.M21_HMS.TabD1`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M21_HMS.TabD1`
SET Activity_Synonym ="HMS"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M21_HMS.TabD1`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M21_HMS.TabD1`
SET Activity ="Height_Measurement"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M21_HMS.TabD1`
ADD column  f1 STRING ;
update `your-project-id1234.M21_HMS.TabD1`
set f1="Height" 
where f1 is null;
#################################################
create table `your-project-id1234.M21_HMS.TabD2`   as
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f1 as feature , Height as value
from  `your-project-id1234.M21_HMS.TabD1` ;
#################################################
alter table `your-project-id1234.M21_HMS.TabD2`
add column num INT64;
update`your-project-id1234.M21_HMS.TabD2`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M21_HMS.TabD3`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M21_HMS.TabD2`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M21_HMS.TabD2`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;






ALTER TABLE `your-project-id1234.M21_HMS.TabD3`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M21_HMS.TabD3`
SET Activity_Value_ID = concat("hms",RankN)
WHERE Activity_Value_ID is null ;



CREATE TABLE `your-project-id1234.M21_HMS.TabD4`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234..M21_HMS.TabD3`  ;

CREATE TABLE `your-project-id1234.M21_HMS.TabD5`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234..M21_HMS.TabD3`  ;




ALTER TABLE `your-project-id1234.M21_HMS.TabD5`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M21_HMS.TabD5`
SET Activity_Synonym = "HMS"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M21_HMS.TabD5`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M21_HMS.TabD5`
SET num = 1
WHERE num is null ;




CREATE TABLE `your-project-id1234.M21_HMS.TabD6`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M21_HMS.TabD5`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M21_HMS.TabD5`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M21_HMS.TabD7`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M21_HMS.TabD6`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M21_HMS.TabD8`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M21_HMS.TabD7`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M21_HMS.TabD9`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M21_HMS.TabD4`   as a
LEFT JOIN `your-project-id1234.M21_HMS.TabD8`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M21_HMS.TabD10`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M21_HMS.TabD6`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M21_HMS.TabD11`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M21_HMS.TabD5`  )    ;



CREATE TABLE `your-project-id1234.M21_HMS.TabD12`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M21_HMS.TabD7`    as a
LEFT JOIN   `your-project-id1234.M21_HMS.TabD8`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
