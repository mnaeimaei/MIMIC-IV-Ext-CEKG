import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M07_TIO` ;
create table `your-project-id1234.M07_TIO.TabA1` as
SELECT * FROM `your-project-id1234.S_Transfers.1_Transfer`
where eventtype="transfer";
create table `your-project-id1234.M07_TIO.TabB1` as
SELECT * FROM `your-project-id1234.S_icuStays.1_icuStays` ;
create table `your-project-id1234.M07_TIO.TabC1` as
SELECT * FROM `your-project-id1234.S_Services.1_Service` ;




create table `your-project-id1234.M07_TIO.Temp`   as
Select * from `your-project-id1234.M07_TIO.TabA1`  ;
ALTER TABLE `your-project-id1234.M07_TIO.Temp`
ADD column  c1 STRING ;
UPDATE `your-project-id1234.M07_TIO.Temp`
SET c1 ="A"
WHERE c1 is null ;
create table `your-project-id1234.M07_TIO.TabA2`   as
Select 
subject_id, hadm_id, transfer_id, eventtype, careunit, intime, outtime, Row_number() over (partition by c1 order by subject_id) as careUnit_transfer_ID,
from `your-project-id1234.M07_TIO.Temp`  ;
drop table `your-project-id1234.M07_TIO.Temp`  ;
###############################################################################################
create table `your-project-id1234.M07_TIO.Temp`   as
Select * from `your-project-id1234.M07_TIO.TabB1`  ;
ALTER TABLE `your-project-id1234.M07_TIO.Temp`
ADD column  c1 STRING ;
UPDATE `your-project-id1234.M07_TIO.Temp`
SET c1 ="A"
WHERE c1 is null ;
create table `your-project-id1234.M07_TIO.TabB2`   as
Select 
subject_id, hadm_id, stay_id, first_careunit, last_careunit, intime, outtime, los, Row_number() over (partition by c1 order by subject_id) as careUnit_transfer_ID,
from `your-project-id1234.M07_TIO.Temp`  ;
drop table `your-project-id1234.M07_TIO.Temp`  ;
###############################################################################################
create table `your-project-id1234.M07_TIO.TabC2` as
SELECT * FROM `your-project-id1234.M07_TIO.TabC1` ;
alter table `your-project-id1234.M07_TIO.TabC2`
add column  careUnit_transfer_ID int64;
update  `your-project-id1234.M07_TIO.TabC2`
set careUnit_transfer_ID = 0
where careUnit_transfer_ID is null ;



update  `your-project-id1234.M07_TIO.TabA2`
set careunit="Hhospital"
where eventtype="discharge" ;
alter table `your-project-id1234.M07_TIO.TabA2`
add column feature1 string;
alter table `your-project-id1234.M07_TIO.TabA2`
add column feature2 string;
update  `your-project-id1234.M07_TIO.TabA2`
set feature1="in_to"
where feature1 is null;
update  `your-project-id1234.M07_TIO.TabA2`
set feature2="out_of"
where feature2 is null;
alter table `your-project-id1234.M07_TIO.TabA2`
add column Activity string;
alter table `your-project-id1234.M07_TIO.TabA2`
add column Activity_Synonym string;
update  `your-project-id1234.M07_TIO.TabA2`
set Activity="Transfer_Careunit"
where Activity is null;
update  `your-project-id1234.M07_TIO.TabA2`
set Activity_Synonym="TIO"
where Activity_Synonym is null;
#################################################################
alter table `your-project-id1234.M07_TIO.TabB2`
add column feature1 string;
alter table `your-project-id1234.M07_TIO.TabB2`
add column feature2 string;
update  `your-project-id1234.M07_TIO.TabB2`
set feature1="in_to"
where feature1 is null;
update  `your-project-id1234.M07_TIO.TabB2`
set feature2="out_of"
where feature2 is null;
alter table `your-project-id1234.M07_TIO.TabB2`
add column Activity string;
alter table `your-project-id1234.M07_TIO.TabB2`
add column Activity_Synonym string;
update  `your-project-id1234.M07_TIO.TabB2`
set Activity="Transfer_Careunit"
where Activity is null;
update  `your-project-id1234.M07_TIO.TabB2`
set Activity_Synonym="TIO"
where Activity_Synonym is null;
#################################################################
alter table `your-project-id1234.M07_TIO.TabC2`
add column feature1 string;
alter table `your-project-id1234.M07_TIO.TabC2`
add column feature2 string;
update  `your-project-id1234.M07_TIO.TabC2`
set feature1="in_to"
where feature1 is null;
update  `your-project-id1234.M07_TIO.TabC2`
set feature2="out_of"
where feature2 is null;
alter table `your-project-id1234.M07_TIO.TabC2`
add column Activity string;
alter table `your-project-id1234.M07_TIO.TabC2`
add column Activity_Synonym string;
update  `your-project-id1234.M07_TIO.TabC2`
set Activity="Transfer_Careunit"
where Activity is null;
update  `your-project-id1234.M07_TIO.TabC2`
set Activity_Synonym="TIO"
where Activity_Synonym is null;



create table `your-project-id1234.M07_TIO.TabA3` as
select distinct * from(
SELECT subject_id, hadm_id ,Activity,Activity_Synonym, intime as Timestamps,
feature1 as careUnit_trnasfer_status, careunit, careUnit_transfer_ID
FROM `your-project-id1234.M07_TIO.TabA2` 
union all
SELECT subject_id, hadm_id ,Activity,Activity_Synonym,  outtime as Timestamps,
feature2 as careUnit_trnasfer_status, careunit, careUnit_transfer_ID
FROM `your-project-id1234.M07_TIO.TabA2`)
;
#####################################################################3
create table `your-project-id1234.M07_TIO.TabB3` as
select distinct * from(
SELECT subject_id, hadm_id ,Activity,Activity_Synonym, intime as Timestamps,
feature1 as careUnit_trnasfer_status, first_careunit as careunit, careUnit_transfer_ID
FROM `your-project-id1234.M07_TIO.TabB2` 
union all
SELECT subject_id, hadm_id ,Activity,Activity_Synonym,  outtime as Timestamps,
feature2 as careUnit_trnasfer_status, last_careunit as careunit, careUnit_transfer_ID
FROM `your-project-id1234.M07_TIO.TabB2`)
;
#####################################################################3
create table `your-project-id1234.M07_TIO.TabC3` as
select distinct * from(
SELECT subject_id, hadm_id ,Activity,Activity_Synonym, transfertime as Timestamps,
feature1 as careUnit_trnasfer_status, prev_service as careunit, careUnit_transfer_ID
FROM `your-project-id1234.M07_TIO.TabC2` 
where prev_service is not null
union all
SELECT subject_id, hadm_id ,Activity,Activity_Synonym,  transfertime as Timestamps,
feature2 as careUnit_trnasfer_status, curr_service as careunit, careUnit_transfer_ID
FROM `your-project-id1234.M07_TIO.TabC2`
where curr_service is not null
) ;



create table `your-project-id1234.M07_TIO.TabD4` as
SELECT 
subject_id, hadm_id, Activity, Activity_Synonym, Timestamps, careunit, careUnit_transfer_ID, careUnit_trnasfer_status
FROM `your-project-id1234.M07_TIO.TabA3` 
union all
SELECT 
subject_id, hadm_id, Activity, Activity_Synonym, Timestamps, careunit, careUnit_transfer_ID, careUnit_trnasfer_status
FROM `your-project-id1234.M07_TIO.TabB3` 
union all
SELECT 
subject_id, hadm_id, Activity, Activity_Synonym, Timestamps, careunit, careUnit_transfer_ID, careUnit_trnasfer_status
FROM `your-project-id1234.M07_TIO.TabC3` ;



create table `your-project-id1234.M07_TIO.TabD5` as
SELECT 
subject_id, hadm_id, Timestamps, Activity, Activity_Synonym
FROM `your-project-id1234.M07_TIO.TabD4`  ;




create table `your-project-id1234.M07_TIO.Temp`   as
Select * from `your-project-id1234.M07_TIO.TabD5`  ;

ALTER TABLE `your-project-id1234.M07_TIO.Temp`
ADD column  c1 STRING ;

UPDATE `your-project-id1234.M07_TIO.Temp`
SET c1 ="A"
WHERE c1 is null ;

create table `your-project-id1234.M07_TIO.Temp2`   as
Select
Row_number() over (partition by c1 order by subject_id, hadm_id, Timestamps, Activity, Activity_Synonym) as NewID, subject_id, hadm_id, Timestamps, Activity, Activity_Synonym
from `your-project-id1234.M07_TIO.Temp`  ;

ALTER TABLE `your-project-id1234.M07_TIO.Temp2`
ADD column  eventID STRING ;

UPDATE `your-project-id1234.M07_TIO.Temp2`
SET eventID = concat("aic",NewID)
WHERE eventID is null ;

create table `your-project-id1234.M07_TIO.TabD6`   as
Select
subject_id, hadm_id, Timestamps, Activity, Activity_Synonym, eventID as Activity_Value_ID
from `your-project-id1234.M07_TIO.Temp2`  ;

drop table `your-project-id1234.M07_TIO.Temp`  ;
drop table `your-project-id1234.M07_TIO.Temp2`  ;




CREATE TABLE  `your-project-id1234.M07_TIO.TabD7`   AS
SELECT * FROM (
SELECT
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.careunit , b.careUnit_transfer_ID , b.careUnit_trnasfer_status,
From `your-project-id1234.M07_TIO.TabD6`   as a
LEFT JOIN `your-project-id1234.M07_TIO.TabD4`   as b
ON
a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id AND a.Timestamps=b.Timestamps AND a.Activity=b.Activity AND a.Activity_Synonym=b.Activity_Synonym )
;



CREATE TABLE  `your-project-id1234.M07_TIO.TabD8`   AS
SELECT 
Activity_Value_ID, Activity, 
careUnit_trnasfer_status as featureName1,
careunit as featureName2,  
CAST(careUnit_transfer_ID AS STRING) AS featureName3,
careUnit_trnasfer_status as featureValue1,
careunit as featureValue2 ,
CAST(careUnit_transfer_ID AS STRING) AS featureValue3
FROM `your-project-id1234.M07_TIO.TabD7` ;
update  `your-project-id1234.M07_TIO.TabD8`
set featureName1="careUnit_trnasfer_status"
where featureName1 is not null;
update  `your-project-id1234.M07_TIO.TabD8`
set featureName2="careunit"
where featureName2 is not null;
update  `your-project-id1234.M07_TIO.TabD8`
set featureName3="careUnit_transfer_ID"
where featureName3 is not null;



CREATE TABLE  `your-project-id1234.M07_TIO.TabD9`   AS
SELECT 
Activity_Value_ID, Activity,
featureName1 as featureName,
featureValue1 as featureValue
FROM `your-project-id1234.M07_TIO.TabD8` 
union all
SELECT
Activity_Value_ID, Activity,
featureName2 as featureName,
featureValue2 as featureValue
FROM `your-project-id1234.M07_TIO.TabD8` 
union all
SELECT
Activity_Value_ID, Activity,
featureName3 as featureName,
featureValue3 as featureValue
FROM `your-project-id1234.M07_TIO.TabD8` ;



ALTER TABLE `your-project-id1234.M07_TIO.TabD9`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M07_TIO.TabD9`
SET Activity_Synonym = "TIO"
WHERE Activity_Synonym is null ;

ALTER TABLE `your-project-id1234.M07_TIO.TabD9`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M07_TIO.TabD9`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M07_TIO.TabD10`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M07_TIO.TabD9`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M07_TIO.TabD9`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;



CREATE TABLE `your-project-id1234.M07_TIO.TabD11`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M07_TIO.TabD10`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M07_TIO.TabD12`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M07_TIO.TabD11`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M07_TIO.TabD13`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M07_TIO.TabD6`   as a
LEFT JOIN `your-project-id1234.M07_TIO.TabD12`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M07_TIO.TabD14`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M07_TIO.TabD10`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M07_TIO.TabD15`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M07_TIO.TabD9`  )    ;



CREATE TABLE `your-project-id1234.M07_TIO.TabD16`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M07_TIO.TabD11`    as a
LEFT JOIN   `your-project-id1234.M07_TIO.TabD12`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;





'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
