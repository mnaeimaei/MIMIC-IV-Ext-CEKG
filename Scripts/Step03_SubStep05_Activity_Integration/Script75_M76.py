import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M76_KDIGO` ;
create table `your-project-id1234.M76_KDIGO.TabA1` as
SELECT * FROM `your-project-id1234.N05_KDIGO.TabC2` ;



CREATE TABLE  `your-project-id1234.M76_KDIGO.TabA2`   AS
SELECT distinct   
subject_id, hadm_id, charttime, creat, creat_low_past_48hr, creat_low_past_7day, aki_stage_creat, aki_stage_uo, aki_stage_crrt, aki_stage, aki_stage_smoothed, weight, urineoutput_6hr, urineoutput_12hr, urineoutput_24hr, uo_rt_6hr, uo_rt_12hr, uo_rt_24hr, uo_tm_6hr, uo_tm_12hr, uo_tm_24hr  
From `your-project-id1234.M76_KDIGO.TabA1` ;



ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M76_KDIGO.TabA2`
SET Activity_Synonym ="KDIGO"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M76_KDIGO.TabA2`
SET Activity ="Acute_Kidney_Injury_Monitoring"
WHERE Activity is null ;





ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f1 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f1="creat" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f2 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f2="creat_low_past_48hr" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f3 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f3="creat_low_past_7day" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f4 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f4="aki_stage_creat" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f5 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f5="aki_stage_uo" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f6 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f6="aki_stage_crrt" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f7 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f7="aki_stage" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f8 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f8="aki_stage_smoothed" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f9 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f9="weight" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f10 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f10="urineoutput_6hr" 
where f10 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f11 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f11="urineoutput_12hr" 
where f11 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f12 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f12="urineoutput_24hr" 
where f12 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f13 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f13="uo_rt_6hr" 
where f13 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f14 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f14 ="uo_rt_12hr" 
where f14 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f15 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f15 ="uo_rt_24hr" 
where f15 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f16 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f16 ="uo_tm_6hr" 
where f16 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f17 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f17 ="uo_tm_12hr" 
where f17 is null;
#################################################
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA2`
ADD column  f18 STRING ;
update `your-project-id1234.M76_KDIGO.TabA2`
set f18 ="uo_tm_24hr" 
where f18 is null;
################################################################

create table `your-project-id1234.M76_KDIGO.TabA3`   as
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(creat AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(creat_low_past_48hr AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(creat_low_past_7day AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(aki_stage_creat AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(aki_stage_uo AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(aki_stage_crrt AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(aki_stage AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(aki_stage_smoothed AS STRING) as  value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(weight AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(urineoutput_6hr AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f11 as feature , CAST(urineoutput_12hr AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f12 as feature , CAST(urineoutput_24hr AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f13 as feature , CAST(uo_rt_6hr AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f14 as feature , CAST(uo_rt_12hr AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f15 as feature , CAST(uo_rt_24hr AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f16 as feature , CAST(uo_tm_6hr AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f17 as feature , CAST(uo_tm_12hr AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f18 as feature , CAST(uo_tm_24hr AS STRING) as value
from  `your-project-id1234.M76_KDIGO.TabA2` ;
#################################################
alter table `your-project-id1234.M76_KDIGO.TabA3`
add column num INT64;
update`your-project-id1234.M76_KDIGO.TabA3`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M76_KDIGO.TabA4`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M76_KDIGO.TabA3`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M76_KDIGO.TabA3`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;





ALTER TABLE `your-project-id1234.M76_KDIGO.TabA4`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M76_KDIGO.TabA4`
SET Activity_Value_ID = concat("kdigo",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M76_KDIGO.TabA5`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M76_KDIGO.TabA4`  ;

CREATE TABLE `your-project-id1234.M76_KDIGO.TabA6`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M76_KDIGO.TabA4`  ;



ALTER TABLE `your-project-id1234.M76_KDIGO.TabA6`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M76_KDIGO.TabA6`
SET Activity_Synonym = "KDIGO"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M76_KDIGO.TabA6`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M76_KDIGO.TabA6`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M76_KDIGO.TabA7`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M76_KDIGO.TabA6`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M76_KDIGO.TabA6`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;



CREATE TABLE `your-project-id1234.M76_KDIGO.TabA8`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M76_KDIGO.TabA7`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M76_KDIGO.TabA9`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M76_KDIGO.TabA8`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M76_KDIGO.TabA10`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M76_KDIGO.TabA5`   as a
LEFT JOIN `your-project-id1234.M76_KDIGO.TabA9`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M76_KDIGO.TabA11`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M76_KDIGO.TabA7`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M76_KDIGO.TabA12`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M76_KDIGO.TabA6`  )    ;



CREATE TABLE `your-project-id1234.M76_KDIGO.TabA13`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M76_KDIGO.TabA8`    as a
LEFT JOIN   `your-project-id1234.M76_KDIGO.TabA9`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;






'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
