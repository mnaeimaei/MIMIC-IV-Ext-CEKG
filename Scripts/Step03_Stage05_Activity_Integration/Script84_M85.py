import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M85_FVTS` ;
create table `your-project-id1234.M85_FVTS.TabA1` as
SELECT  * FROM `your-project-id1234.N11_Vital.first_day_vitalsign_time` ;



CREATE TABLE  `your-project-id1234.M85_FVTS.TabA2`   AS
SELECT distinct * FROM (
SELECT
a.subject_id , b.hadm_id, a.stay_id , a.Timestamps , a.heart_rate_min , a.heart_rate_max , a.heart_rate_mean , a.sbp_min , a.sbp_max , a.sbp_mean , a.dbp_min , a.dbp_max , a.dbp_mean , a.mbp_min , a.mbp_max , a.mbp_mean , a.resp_rate_min , a.resp_rate_max , a.resp_rate_mean , a.temperature_min , a.temperature_max , a.temperature_mean , a.spo2_min , a.spo2_max , a.spo2_mean , a.glucose_min , a.glucose_max , a.glucose_mean,

From `your-project-id1234.M85_FVTS.TabA1`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SHI`   as b
ON
a.subject_id=b.subject_id AND a.stay_id=b.stay_id
AND  a.Timestamps>=b.min AND a.Timestamps<=b.max
)
;



CREATE TABLE  `your-project-id1234.M85_FVTS.TabA3`   AS
SELECT distinct   
subject_id, hadm_id, Timestamps, heart_rate_min, heart_rate_max, heart_rate_mean, sbp_min, sbp_max, sbp_mean, dbp_min, dbp_max, dbp_mean, mbp_min, mbp_max, mbp_mean, resp_rate_min, resp_rate_max, resp_rate_mean, temperature_min, temperature_max, temperature_mean, spo2_min, spo2_max, spo2_mean, glucose_min, glucose_max, glucose_mean  
From `your-project-id1234.M85_FVTS.TabA2` ;



ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M85_FVTS.TabA3`
SET Activity_Synonym ="FVTS"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M85_FVTS.TabA3`
SET Activity ="First_day_vitalsign"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f1 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f1="heart_rate_min" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f2 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f2="heart_rate_max" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f3 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f3="heart_rate_mean" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f4 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f4="sbp_min" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f5 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f5="sbp_max" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f6 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f6="sbp_mean" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f7 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f7="dbp_min" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f8 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f8="dbp_max" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f9 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f9="dbp_mean" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f10 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f10="mbp_min" 
where f10 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f11 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f11="mbp_max" 
where f11 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f12 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f12="mbp_mean" 
where f12 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f13 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f13="resp_rate_min" 
where f13 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f14 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f14 ="resp_rate_max" 
where f14 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f15 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f15 ="resp_rate_mean" 
where f15 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f16 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f16 ="temperature_min" 
where f16 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f17 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f17 ="temperature_max" 
where f17 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f18 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f18 ="temperature_mean" 
where f18 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f19 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f19 ="spo2_min" 
where f19 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f20 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f20 ="spo2_max" 
where f20 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f21 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f21 ="spo2_mean" 
where f21 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f22 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f22 ="glucose_min" 
where f22 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f23 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f23 ="glucose_max" 
where f23 is null;
#################################################
ALTER TABLE `your-project-id1234.M85_FVTS.TabA3`
ADD column  f24 STRING ;
update `your-project-id1234.M85_FVTS.TabA3`
set f24 ="glucose_mean" 
where f24 is null;
################################################################

create table `your-project-id1234.M85_FVTS.TabA4`   as
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(heart_rate_min AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(heart_rate_max AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(heart_rate_mean AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(sbp_min AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(sbp_max AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(sbp_mean AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(dbp_min AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(dbp_max AS STRING) as  value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(dbp_mean AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(mbp_min AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f11 as feature , CAST(mbp_max AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f12 as feature , CAST(mbp_mean AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f13 as feature , CAST(resp_rate_min AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f14 as feature , CAST(resp_rate_max AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f15 as feature , CAST(resp_rate_mean AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f16 as feature , CAST(temperature_min AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f17 as feature , CAST(temperature_max AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f18 as feature , CAST(temperature_mean AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f19 as feature , CAST(spo2_min AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f20 as feature , CAST(spo2_max AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f21 as feature , CAST(spo2_mean AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f22 as feature , CAST(glucose_min AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f23 as feature , CAST(glucose_max AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f24 as feature , CAST(glucose_mean AS STRING) as value
from  `your-project-id1234.M85_FVTS.TabA3` ;
#################################################
alter table `your-project-id1234.M85_FVTS.TabA4`
add column num INT64;
update`your-project-id1234.M85_FVTS.TabA4`
set num=1
where num is null;




CREATE TABLE `your-project-id1234.M85_FVTS.TabA5`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M85_FVTS.TabA4`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M85_FVTS.TabA4`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;






ALTER TABLE `your-project-id1234.M85_FVTS.TabA5`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M85_FVTS.TabA5`
SET Activity_Value_ID = concat("fvts",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M85_FVTS.TabA6`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M85_FVTS.TabA5`  ;

CREATE TABLE `your-project-id1234.M85_FVTS.TabA7`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M85_FVTS.TabA5`  ;




ALTER TABLE `your-project-id1234.M85_FVTS.TabA7`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M85_FVTS.TabA7`
SET Activity_Synonym = "FVTS"
WHERE Activity_Synonym is null ;

ALTER TABLE `your-project-id1234.M85_FVTS.TabA7`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M85_FVTS.TabA7`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M85_FVTS.TabA8`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M85_FVTS.TabA7`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M85_FVTS.TabA7`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M85_FVTS.TabA9`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M85_FVTS.TabA8`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M85_FVTS.TabA10`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M85_FVTS.TabA9`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M85_FVTS.TabA11`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M85_FVTS.TabA6`   as a
LEFT JOIN `your-project-id1234.M85_FVTS.TabA10`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M85_FVTS.TabA12`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M85_FVTS.TabA8`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M85_FVTS.TabA13`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M85_FVTS.TabA7`  )    ;



CREATE TABLE `your-project-id1234.M85_FVTS.TabA14`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M85_FVTS.TabA9`    as a
LEFT JOIN   `your-project-id1234.M85_FVTS.TabA10`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
