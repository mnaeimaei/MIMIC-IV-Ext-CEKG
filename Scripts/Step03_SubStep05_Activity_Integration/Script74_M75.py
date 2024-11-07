import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M75_NBM` ;
create table `your-project-id1234.M75_NBM.TabA1` as
SELECT * FROM `your-project-id1234.S_Derived1.neuroblock` ;



CREATE TABLE  `your-project-id1234.M75_NBM.TabA2`   AS
SELECT * FROM (
SELECT
b.subject_id , b.hadm_id, b.stay_id ,  a.starttime , a.endtime, a.orderid as solutionID,
b.label as drug ,  a.drug_amount ,  b.amountuom as drug_amount_uom , b.originalrate as original_drug_rate , a.drug_rate , b.rateuom as drug_rate_uom ,   b.statusdescription as status_description 
From `your-project-id1234.M75_NBM.TabA1`   as a
LEFT JOIN `your-project-id1234.S_inputEvents.1_inputEvent`   as b
ON
a.orderid=b.orderid )    
; 





create table `your-project-id1234.M75_NBM.Temp`   as
Select * from `your-project-id1234.M75_NBM.TabA2`  ;

ALTER TABLE `your-project-id1234.M75_NBM.Temp`
ADD column  c1 STRING ;

UPDATE `your-project-id1234.M75_NBM.Temp`
SET c1 ="A"
WHERE c1 is null ;

create table `your-project-id1234.M75_NBM.TabA3`   as
Select
subject_id, hadm_id, stay_id, starttime, endtime, Row_number() over (partition by c1 order by subject_id) as NBM_ID, solutionID, drug, drug_amount, drug_amount_uom, original_drug_rate, drug_rate, drug_rate_uom, status_description
from `your-project-id1234.M75_NBM.Temp`  ;

drop table `your-project-id1234.M75_NBM.Temp`  ;





alter table `your-project-id1234.M75_NBM.TabA3`
add column ss string;

alter table `your-project-id1234.M75_NBM.TabA3`
add column ee string;

update `your-project-id1234.M75_NBM.TabA3`
set ss="start"
where ss is null and starttime is not null;

update `your-project-id1234.M75_NBM.TabA3`
set ee="end"
where ee is null and endtime is not null;



CREATE TABLE  `your-project-id1234.M75_NBM.TabA4`  as 

SELECT subject_id, hadm_id, stay_id, 
starttime as Timestamps , 
NBM_ID , 
ss as NBM_Status , 
solutionID, drug, drug_amount, drug_amount_uom, original_drug_rate, drug_rate, drug_rate_uom, status_description
FROM `your-project-id1234.M75_NBM.TabA3`  
union all
SELECT subject_id, hadm_id, stay_id, 
endtime as Timestamps , 
NBM_ID , 
ee  as NBM_Status , 
solutionID, drug, drug_amount, drug_amount_uom, original_drug_rate, drug_rate, drug_rate_uom, status_description
FROM `your-project-id1234.M75_NBM.TabA3`  ;



CREATE TABLE  `your-project-id1234.M75_NBM.TabA5`   AS
SELECT distinct   
subject_id, hadm_id, Timestamps, NBM_ID, NBM_Status, solutionID, drug, drug_amount, drug_amount_uom, original_drug_rate, drug_rate, drug_rate_uom, status_description  
From `your-project-id1234.M75_NBM.TabA4` ;



ALTER TABLE `your-project-id1234.M75_NBM.TabA5`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M75_NBM.TabA5`
SET Activity_Synonym ="NBM"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M75_NBM.TabA5`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M75_NBM.TabA5`
SET Activity ="neurological_blockades_medications"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M75_NBM.TabA5`
ADD column  f1 STRING ;
update `your-project-id1234.M75_NBM.TabA5`
set f1="NBM_ID" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M75_NBM.TabA5`
ADD column  f2 STRING ;
update `your-project-id1234.M75_NBM.TabA5`
set f2="NBM_Status" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M75_NBM.TabA5`
ADD column  f3 STRING ;
update `your-project-id1234.M75_NBM.TabA5`
set f3="solutionID" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M75_NBM.TabA5`
ADD column  f4 STRING ;
update `your-project-id1234.M75_NBM.TabA5`
set f4="drug" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M75_NBM.TabA5`
ADD column  f5 STRING ;
update `your-project-id1234.M75_NBM.TabA5`
set f5="drug_amount" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M75_NBM.TabA5`
ADD column  f6 STRING ;
update `your-project-id1234.M75_NBM.TabA5`
set f6="drug_amount_uom" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M75_NBM.TabA5`
ADD column  f7 STRING ;
update `your-project-id1234.M75_NBM.TabA5`
set f7="original_drug_rate" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M75_NBM.TabA5`
ADD column  f8 STRING ;
update `your-project-id1234.M75_NBM.TabA5`
set f8="drug_rate" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M75_NBM.TabA5`
ADD column  f9 STRING ;
update `your-project-id1234.M75_NBM.TabA5`
set f9="drug_rate_uom" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M75_NBM.TabA5`
ADD column  f10 STRING ;
update `your-project-id1234.M75_NBM.TabA5`
set f10="status_description" 
where f10 is null;
################################################################

create table `your-project-id1234.M75_NBM.TabA6`   as
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(NBM_ID AS STRING) as value
from  `your-project-id1234.M75_NBM.TabA5`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(NBM_Status AS STRING) as value
from  `your-project-id1234.M75_NBM.TabA5`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(solutionID AS STRING) as value
from  `your-project-id1234.M75_NBM.TabA5`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(drug AS STRING) as value
from  `your-project-id1234.M75_NBM.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(drug_amount AS STRING) as value
from  `your-project-id1234.M75_NBM.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(drug_amount_uom AS STRING) as value
from  `your-project-id1234.M75_NBM.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(original_drug_rate AS STRING) as value
from  `your-project-id1234.M75_NBM.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(drug_rate AS STRING) as  value
from  `your-project-id1234.M75_NBM.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(drug_rate_uom AS STRING) as value
from  `your-project-id1234.M75_NBM.TabA5`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(status_description AS STRING) as value
from  `your-project-id1234.M75_NBM.TabA5`;
#################################################
alter table `your-project-id1234.M75_NBM.TabA6`
add column num INT64;
update`your-project-id1234.M75_NBM.TabA6`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M75_NBM.TabA7`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M75_NBM.TabA6`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M75_NBM.TabA6`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;






ALTER TABLE `your-project-id1234.M75_NBM.TabA7`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M75_NBM.TabA7`
SET Activity_Value_ID = concat("nbm",RankN)
WHERE Activity_Value_ID is null ;



CREATE TABLE `your-project-id1234.M75_NBM.TabA8`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M75_NBM.TabA7`  ;

CREATE TABLE `your-project-id1234.M75_NBM.TabA9`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M75_NBM.TabA7`  ;



ALTER TABLE `your-project-id1234.M75_NBM.TabA9`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M75_NBM.TabA9`
SET Activity_Synonym = "NBM"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M75_NBM.TabA9`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M75_NBM.TabA9`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M75_NBM.TabA10`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M75_NBM.TabA9`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M75_NBM.TabA9`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M75_NBM.TabA11`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M75_NBM.TabA10`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M75_NBM.TabA12`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M75_NBM.TabA11`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M75_NBM.TabA13`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M75_NBM.TabA8`   as a
LEFT JOIN `your-project-id1234.M75_NBM.TabA12`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M75_NBM.TabA14`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M75_NBM.TabA10`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M75_NBM.TabA15`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M75_NBM.TabA9`  )    ;



CREATE TABLE `your-project-id1234.M75_NBM.TabA16`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M75_NBM.TabA11`    as a
LEFT JOIN   `your-project-id1234.M75_NBM.TabA12`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;






'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
