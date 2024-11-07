import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M89_VTR` ;
create table `your-project-id1234.M89_VTR.TabA1` as
SELECT  * FROM `your-project-id1234.N15_vent.ventilator_setting` ;



CREATE TABLE  `your-project-id1234.M89_VTR.TabA2`   AS
SELECT distinct * FROM (
SELECT
a.subject_id , b.hadm_id, a.stay_id , a.charttime , a.respiratory_rate_set , a.respiratory_rate_total , a.respiratory_rate_spontaneous , a.minute_volume , a.tidal_volume_set , a.tidal_volume_observed , a.tidal_volume_spontaneous , a.plateau_pressure , a.peep , a.fio2 , a.flow_rate , a.ventilator_mode , a.ventilator_mode_hamilton , a.ventilator_type,

From `your-project-id1234.M89_VTR.TabA1`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SHI`   as b
ON
a.subject_id=b.subject_id AND a.stay_id=b.stay_id
AND  a.charttime>=b.min AND a.charttime<=b.max
)
;



CREATE TABLE  `your-project-id1234.M89_VTR.TabA3`   AS
SELECT distinct   
subject_id, hadm_id, charttime, respiratory_rate_set, respiratory_rate_total, respiratory_rate_spontaneous, minute_volume, tidal_volume_set, tidal_volume_observed, tidal_volume_spontaneous, plateau_pressure, peep, fio2, flow_rate, ventilator_mode, ventilator_mode_hamilton, ventilator_type  
From `your-project-id1234.M89_VTR.TabA2` ;



ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M89_VTR.TabA3`
SET Activity_Synonym ="VTR"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M89_VTR.TabA3`
SET Activity ="ventilator_setting"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f1 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f1="respiratory_rate_set" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f2 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f2="respiratory_rate_total" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f3 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f3="respiratory_rate_spontaneous" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f4 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f4="minute_volume" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f5 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f5="tidal_volume_set" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f6 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f6="tidal_volume_observed" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f7 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f7="tidal_volume_spontaneous" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f8 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f8="plateau_pressure" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f9 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f9="peep" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f10 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f10="fio2" 
where f10 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f11 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f11="flow_rate" 
where f11 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f12 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f12="ventilator_mode" 
where f12 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f13 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f13="ventilator_mode_hamilton" 
where f13 is null;
#################################################
ALTER TABLE `your-project-id1234.M89_VTR.TabA3`
ADD column  f14 STRING ;
update `your-project-id1234.M89_VTR.TabA3`
set f14 ="ventilator_type" 
where f14 is null;
#################################################
################################################################

create table `your-project-id1234.M89_VTR.TabA4`   as
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(respiratory_rate_set AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(respiratory_rate_total AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(respiratory_rate_spontaneous AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(minute_volume AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(tidal_volume_set AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(tidal_volume_observed AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(tidal_volume_spontaneous AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(plateau_pressure AS STRING) as  value
from  `your-project-id1234.M89_VTR.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(peep AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(fio2 AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f11 as feature , CAST(flow_rate AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f12 as feature , CAST(ventilator_mode AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f13 as feature , CAST(ventilator_mode_hamilton AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f14 as feature , CAST(ventilator_type AS STRING) as value
from  `your-project-id1234.M89_VTR.TabA3`  ;
#################################################
alter table `your-project-id1234.M89_VTR.TabA4`
add column num INT64;
update`your-project-id1234.M89_VTR.TabA4`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M89_VTR.TabA5`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M89_VTR.TabA4`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M89_VTR.TabA4`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;





ALTER TABLE `your-project-id1234.M89_VTR.TabA5`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M89_VTR.TabA5`
SET Activity_Value_ID = concat("vtr",RankN)
WHERE Activity_Value_ID is null ;



CREATE TABLE `your-project-id1234.M89_VTR.TabA6`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M89_VTR.TabA5`  ;

CREATE TABLE `your-project-id1234.M89_VTR.TabA7`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M89_VTR.TabA5`  ;



ALTER TABLE `your-project-id1234.M89_VTR.TabA7`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M89_VTR.TabA7`
SET Activity_Synonym = "VTR"
WHERE Activity_Synonym is null ;

ALTER TABLE `your-project-id1234.M89_VTR.TabA7`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M89_VTR.TabA7`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M89_VTR.TabA8`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M89_VTR.TabA7`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M89_VTR.TabA7`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M89_VTR.TabA9`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M89_VTR.TabA8`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M89_VTR.TabA10`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M89_VTR.TabA9`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M89_VTR.TabA11`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M89_VTR.TabA6`   as a
LEFT JOIN `your-project-id1234.M89_VTR.TabA10`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M89_VTR.TabA12`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M89_VTR.TabA8`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M89_VTR.TabA13`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M89_VTR.TabA7`  )    ;



CREATE TABLE `your-project-id1234.M89_VTR.TabA14`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M89_VTR.TabA9`    as a
LEFT JOIN   `your-project-id1234.M89_VTR.TabA10`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;






'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
