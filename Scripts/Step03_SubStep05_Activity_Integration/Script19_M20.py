import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M20_WMS` ;
create table `your-project-id1234.M20_WMS.TabA1` as
SELECT * FROM `your-project-id1234.N03_OMR.01_Weight` ;
create table `your-project-id1234.M20_WMS.TabC1` as
SELECT * FROM `your-project-id1234.N13_weight.weight_final` ;



create table `your-project-id1234.M20_WMS.TabA2` as
SELECT * FROM `your-project-id1234.M20_WMS.TabA1` ;
create table `your-project-id1234.M20_WMS.TabC2` as
SELECT * FROM `your-project-id1234.M20_WMS.TabC1` ;



#######################################################################
alter table `your-project-id1234.M20_WMS.TabA2`
add column  weight_type string;
alter table `your-project-id1234.M20_WMS.TabA2`
add column  weight_ID string;
alter table `your-project-id1234.M20_WMS.TabA2`
add column  weight_status string;
update `your-project-id1234.M20_WMS.TabA2`
set weight_ID = '0'
where weight_ID is null;
update `your-project-id1234.M20_WMS.TabA2`
set weight_status = 'OneTime'
where weight_status is null;
CREATE TABLE  `your-project-id1234.M20_WMS.TabA3`   AS
SELECT subject_id, hadm_id, Timestamps, weight_type, result_value as weight, weight_ID, weight_status
FROM `your-project-id1234.M20_WMS.TabA2` ;
#########################################################################
create table `your-project-id1234.M20_WMS.Temp`   as
Select * from `your-project-id1234.M20_WMS.TabC2`  ;
ALTER TABLE `your-project-id1234.M20_WMS.Temp`
ADD column  c1 STRING ;
UPDATE `your-project-id1234.M20_WMS.Temp`
SET c1 ="A"
WHERE c1 is null ;
create table `your-project-id1234.M20_WMS.TabC3`   as
Select 
subject_id, stay_id, starttime, endtime, weight_type, weight, Row_number() over (partition by c1 order by subject_id) as weight_ID, 
from `your-project-id1234.M20_WMS.Temp`  ;
drop table `your-project-id1234.M20_WMS.Temp`  ;
alter table `your-project-id1234.M20_WMS.TabC3`
add column feature1 string;
alter table `your-project-id1234.M20_WMS.TabC3`
add column feature2 string;
update  `your-project-id1234.M20_WMS.TabC3`
set feature1="start"
where feature1 is null;
update  `your-project-id1234.M20_WMS.TabC3`
set feature2="end"
where feature2 is null;
create table `your-project-id1234.M20_WMS.TabC4` as
select distinct * from(
SELECT subject_id, stay_id , starttime as Timestamps, weight_type, weight,weight_ID,
feature1 as weight_status
FROM `your-project-id1234.M20_WMS.TabC3` 
union all
SELECT subject_id, stay_id , endtime as Timestamps,weight_type, weight,weight_ID,
feature2 as weight_status
FROM `your-project-id1234.M20_WMS.TabC3`)
;
CREATE TABLE  `your-project-id1234.M20_WMS.TabC5`   AS
SELECT distinct * FROM (
SELECT
a.subject_id ,b.hadm_id, a.stay_id , a.Timestamps , a.weight_type , a.weight , a.weight_ID , a.weight_status,
From `your-project-id1234.M20_WMS.TabC4`   as a
LEFT JOIN `your-project-id1234.R_TimeC.SHI`   as b
ON
a.subject_id=b.subject_id AND a.stay_id=b.stay_id 
AND  a.Timestamps>=b.min AND a.Timestamps<=b.max )    
; 
update  `your-project-id1234.M20_WMS.TabC5`
set hadm_id=0
where hadm_id is null;



CREATE TABLE  `your-project-id1234.M20_WMS.TabD1`   AS
Select
subject_id ,hadm_id, Timestamps , weight_type , weight , weight_ID , weight_status
FROM  `your-project-id1234.M20_WMS.TabA3` 
union distinct
Select
subject_id ,hadm_id, Timestamps , weight_type , CAST(weight AS STRING) weight, CAST(weight_ID AS STRING) weight_ID , weight_status
FROM  `your-project-id1234.M20_WMS.TabC5`  ;



alter table  `your-project-id1234.M20_WMS.TabD1`
add column Activity string;
alter table  `your-project-id1234.M20_WMS.TabD1`
add column Activity_Synonym string;
UPDATE `your-project-id1234.M20_WMS.TabD1`
SET Activity ="Weight_Measurement"
WHERE Activity is null ;


UPDATE `your-project-id1234.M20_WMS.TabD1`  
SET Activity_Synonym ="WMS"
WHERE Activity_Synonym is null ;



ALTER TABLE `your-project-id1234.M20_WMS.TabD1`
ADD column  f1 STRING ;
update `your-project-id1234.M20_WMS.TabD1`
set f1="weight_type" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M20_WMS.TabD1`
ADD column  f2 STRING ;
update `your-project-id1234.M20_WMS.TabD1`
set f2="weight" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M20_WMS.TabD1`
ADD column  f3 STRING ;
update `your-project-id1234.M20_WMS.TabD1`
set f3="weight_ID" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M20_WMS.TabD1`
ADD column  f4 STRING ;
update `your-project-id1234.M20_WMS.TabD1`
set f4="weight_status" 
where f4 is null;
#################################################

create table `your-project-id1234.M20_WMS.TabD2`   as
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f1 as feature , weight_type as value
from  `your-project-id1234.M20_WMS.TabD1`  
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f2 as feature , weight as value
from  `your-project-id1234.M20_WMS.TabD1`  
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f3 as feature , weight_ID as value
from  `your-project-id1234.M20_WMS.TabD1`  
union distinct
select subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, f4 as feature , weight_status as value
from  `your-project-id1234.M20_WMS.TabD1`;
#################################################
alter table `your-project-id1234.M20_WMS.TabD2`
add column num INT64;
update`your-project-id1234.M20_WMS.TabD2`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M20_WMS.TabD3`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M20_WMS.TabD2`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M20_WMS.TabD2`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;





ALTER TABLE `your-project-id1234.M20_WMS.TabD3`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M20_WMS.TabD3`
SET Activity_Value_ID = concat("wms",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M20_WMS.TabD4`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234..M20_WMS.TabD3`  ;

CREATE TABLE `your-project-id1234.M20_WMS.TabD5`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234..M20_WMS.TabD3`  ;




ALTER TABLE `your-project-id1234.M20_WMS.TabD5`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M20_WMS.TabD5`
SET Activity_Synonym = "WMS"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M20_WMS.TabD5`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M20_WMS.TabD5`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M20_WMS.TabD6`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M20_WMS.TabD5`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M20_WMS.TabD5`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;



CREATE TABLE `your-project-id1234.M20_WMS.TabD7`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M20_WMS.TabD6`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M20_WMS.TabD8`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M20_WMS.TabD7`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M20_WMS.TabD9`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M20_WMS.TabD4`   as a
LEFT JOIN `your-project-id1234.M20_WMS.TabD8`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M20_WMS.TabD10`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M20_WMS.TabD6`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M20_WMS.TabD11`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M20_WMS.TabD5`  )    ;



CREATE TABLE `your-project-id1234.M20_WMS.TabD12`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M20_WMS.TabD7`    as a
LEFT JOIN   `your-project-id1234.M20_WMS.TabD8`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
