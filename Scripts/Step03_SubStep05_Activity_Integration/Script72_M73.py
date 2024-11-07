import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M73_CRI` ;
create table `your-project-id1234.M73_CRI.TabA1` as
SELECT * FROM `your-project-id1234.S_Derived1.rhythm` ;



CREATE TABLE `your-project-id1234.M73_CRI.TabA3`  AS
SELECT
a.subject_id, a.charttime, a.hadm_id,
b.RankN,
a.heart_rhythm, a.ectopy_type, a.ectopy_frequency, a.ectopy_type_secondary, a.ectopy_frequency_secondary
FROM `your-project-id1234.M73_CRI.TabA2`  a
LEFT   JOIN (
SELECT  
subject_id, charttime, hadm_id,
Row_number() over (partition by  subject_id, charttime order by hadm_id asc) as RankN
FROM
(SELECT   DISTINCT subject_id, charttime, hadm_id  FROM `your-project-id1234.M73_CRI.TabA2`  )  ) b
ON
a.subject_id = b.subject_id AND a.charttime = b.charttime AND a.hadm_id = b.hadm_id;




CREATE TABLE `your-project-id1234.M20_WMS.TabA91`  AS

SELECT
a.subject_id, a.charttime, a.hadm_id,
b.RankN,
a.heart_rhythm, a.ectopy_type, a.ectopy_frequency, a.ectopy_type_secondary, a.ectopy_frequency_secondary
FROM `your-project-id1234.M20_WMS.TabA81`  a
LEFT   JOIN (
SELECT
subject_id, charttime, hadm_id,
Row_number() over (partition by  subject_id, charttime  order by hadm_id asc) as RankN
FROM

(SELECT   DISTINCT subject_id, charttime, hadm_id  FROM `your-project-id1234.M20_WMS.TabA81`  )  ) b
ON
a.subject_id = b.subject_id AND a.charttime = b.charttime AND a.hadm_id = b.hadm_id;




CREATE TABLE `your-project-id1234.M73_CRI.TabA4`  AS
SELECT * FROM `your-project-id1234.M73_CRI.TabA3` 
where RankN=1;



ALTER TABLE `your-project-id1234.M73_CRI.TabA4`  
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M73_CRI.TabA4`
SET Activity_Synonym ="CRI"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M73_CRI.TabA4`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M73_CRI.TabA4`
SET Activity ="cardiac_rhythm_information"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M73_CRI.TabA4`
ADD column  f1 STRING ;
update `your-project-id1234.M73_CRI.TabA4`
set f1="RankN" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M73_CRI.TabA4`
ADD column  f2 STRING ;
update `your-project-id1234.M73_CRI.TabA4`
set f2="heart_rhythm" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M73_CRI.TabA4`
ADD column  f3 STRING ;
update `your-project-id1234.M73_CRI.TabA4`
set f3="ectopy_type" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M73_CRI.TabA4`
ADD column  f4 STRING ;
update `your-project-id1234.M73_CRI.TabA4`
set f4="ectopy_frequency" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M73_CRI.TabA4`
ADD column  f5 STRING ;
update `your-project-id1234.M73_CRI.TabA4`
set f5="ectopy_type_secondary" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M73_CRI.TabA4`
ADD column  f6 STRING ;
update `your-project-id1234.M73_CRI.TabA4`
set f6="ectopy_frequency_secondary" 
where f6 is null;
#################################################
create table `your-project-id1234.M73_CRI.TabA5`   as
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(RankN AS STRING) as value
from  `your-project-id1234.M73_CRI.TabA4`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(heart_rhythm AS STRING) as value
from  `your-project-id1234.M73_CRI.TabA4`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(ectopy_type AS STRING) as value
from  `your-project-id1234.M73_CRI.TabA4`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(ectopy_frequency AS STRING) as value
from  `your-project-id1234.M73_CRI.TabA4`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(ectopy_type_secondary AS STRING) as value
from  `your-project-id1234.M73_CRI.TabA4`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(ectopy_frequency_secondary AS STRING) as value
from  `your-project-id1234.M73_CRI.TabA4` ;
#################################################
alter table `your-project-id1234.M73_CRI.TabA5`
add column num INT64;
update`your-project-id1234.M73_CRI.TabA5`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M73_CRI.TabA6`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M73_CRI.TabA5`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M73_CRI.TabA5`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;





ALTER TABLE `your-project-id1234.M73_CRI.TabA6`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M73_CRI.TabA6`
SET Activity_Value_ID = concat("cri",RankN)
WHERE Activity_Value_ID is null ;



CREATE TABLE `your-project-id1234.M73_CRI.TabA7`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M73_CRI.TabA6`  ;

CREATE TABLE `your-project-id1234.M73_CRI.TabA8`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M73_CRI.TabA6`  ;



ALTER TABLE `your-project-id1234.M73_CRI.TabA8`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M73_CRI.TabA8`
SET Activity_Synonym = "CRI"
WHERE Activity_Synonym is null ;

ALTER TABLE `your-project-id1234.M73_CRI.TabA8`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M73_CRI.TabA8`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M73_CRI.TabA9`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M73_CRI.TabA8`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M73_CRI.TabA8`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M73_CRI.TabA10`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M73_CRI.TabA9`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M73_CRI.TabA11`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M73_CRI.TabA10`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M73_CRI.TabA12`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M73_CRI.TabA7`   as a
LEFT JOIN `your-project-id1234.M73_CRI.TabA11`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M73_CRI.TabA13`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M73_CRI.TabA9`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M73_CRI.TabA14`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M73_CRI.TabA8`  )    ;



CREATE TABLE `your-project-id1234.M73_CRI.TabA15`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M73_CRI.TabA10`    as a
LEFT JOIN   `your-project-id1234.M73_CRI.TabA11`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
