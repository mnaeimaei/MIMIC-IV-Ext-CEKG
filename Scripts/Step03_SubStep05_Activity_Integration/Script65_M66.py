import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M66_CRRT` ;
create table `your-project-id1234.M66_CRRT.TabA1` as
SELECT * FROM `your-project-id1234.S_Derived1.crrt` ;



CREATE TABLE  `your-project-id1234.M66_CRRT.TabA2`   AS
SELECT distinct * FROM (
SELECT
b.subject_id , b.hadm_id ,a.stay_id , a.charttime , a.crrt_mode , a.access_pressure , a.blood_flow , a.citrate , a.current_goal , a.dialysate_fluid , a.dialysate_rate , a.effluent_pressure , a.filter_pressure , a.heparin_concentration , a.heparin_dose , a.hourly_patient_fluid_removal , a.prefilter_replacement_rate , a.postfilter_replacement_rate , a.replacement_fluid , a.replacement_rate , a.return_pressure , a.ultrafiltrate_output , a.system_active , a.clots , a.clots_increasing , a.clotted,
From `your-project-id1234.M66_CRRT.TabA1`   as a
LEFT JOIN `your-project-id1234.R_TimeC.SHI`   as b
ON
a.stay_id=b.stay_id
AND  a.charttime>=b.min AND a.charttime<=b.max
)        ;



CREATE TABLE  `your-project-id1234.M66_CRRT.TabA3`   AS
SELECT distinct   
subject_id, hadm_id, charttime, crrt_mode, access_pressure, blood_flow, citrate, current_goal, dialysate_fluid, dialysate_rate, effluent_pressure, filter_pressure, heparin_concentration, heparin_dose, hourly_patient_fluid_removal, prefilter_replacement_rate, postfilter_replacement_rate, replacement_fluid, replacement_rate, return_pressure, ultrafiltrate_output, system_active, clots, clots_increasing, clotted  
From `your-project-id1234.M66_CRRT.TabA2` ;



ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M66_CRRT.TabA3`
SET Activity_Synonym ="CRRT"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M66_CRRT.TabA3`
SET Activity ="Continuous_renal_replacement_therapy"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f1 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f1="crrt_mode" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f2 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f2="access_pressure" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f3 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f3="blood_flow" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f4 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f4="citrate" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f5 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f5="current_goal" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f6 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f6="dialysate_fluid" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f7 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f7="dialysate_rate" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f8 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f8="effluent_pressure" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f9 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f9="filter_pressure" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f10 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f10="heparin_concentration" 
where f10 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f11 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f11="heparin_dose" 
where f11 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f12 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f12="hourly_patient_fluid_removal" 
where f12 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f13 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f13="prefilter_replacement_rate" 
where f13 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f14 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f14 ="postfilter_replacement_rate" 
where f14 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f15 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f15 ="replacement_fluid" 
where f15 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f16 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f16 ="replacement_rate" 
where f16 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f17 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f17 ="return_pressure" 
where f17 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f18 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f18 ="ultrafiltrate_output" 
where f18 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f19 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f19 ="system_active" 
where f19 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f20 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f20 ="clots" 
where f20 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f21 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f21 ="clots_increasing" 
where f21 is null;
#################################################
ALTER TABLE `your-project-id1234.M66_CRRT.TabA3`
ADD column  f22 STRING ;
update `your-project-id1234.M66_CRRT.TabA3`
set f22 ="clotted" 
where f22 is null;
################################################################

create table `your-project-id1234.M66_CRRT.TabA4`   as
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(crrt_mode AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(access_pressure AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(blood_flow AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(citrate AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(current_goal AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(dialysate_fluid AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(dialysate_rate AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(effluent_pressure AS STRING) as  value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(filter_pressure AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(heparin_concentration AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f11 as feature , CAST(heparin_dose AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f12 as feature , CAST(hourly_patient_fluid_removal AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f13 as feature , CAST(prefilter_replacement_rate AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f14 as feature , CAST(postfilter_replacement_rate AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f15 as feature , CAST(replacement_fluid AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f16 as feature , CAST(replacement_rate AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f17 as feature , CAST(return_pressure AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f18 as feature , CAST(ultrafiltrate_output AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f19 as feature , CAST(system_active AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f20 as feature , CAST(clots AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f21 as feature , CAST(clots_increasing AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f22 as feature , CAST(clotted AS STRING) as value
from  `your-project-id1234.M66_CRRT.TabA3`;
#################################################
alter table `your-project-id1234.M66_CRRT.TabA4`
add column num INT64;
update`your-project-id1234.M66_CRRT.TabA4`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M66_CRRT.TabA5`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M66_CRRT.TabA4`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M66_CRRT.TabA4`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;




ALTER TABLE `your-project-id1234.M66_CRRT.TabA5`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M66_CRRT.TabA5`
SET Activity_Value_ID = concat("crrt",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M66_CRRT.TabA6`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M66_CRRT.TabA5`  ;

CREATE TABLE `your-project-id1234.M66_CRRT.TabA7`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M66_CRRT.TabA5`  ;



ALTER TABLE `your-project-id1234.M66_CRRT.TabA7`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M66_CRRT.TabA7`
SET Activity_Synonym = "CRRT"
WHERE Activity_Synonym is null ;

ALTER TABLE `your-project-id1234.M66_CRRT.TabA7`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M66_CRRT.TabA7`
SET num = 1
WHERE num is null ;




CREATE TABLE `your-project-id1234.M66_CRRT.TabA8`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M66_CRRT.TabA7`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M66_CRRT.TabA7`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;



CREATE TABLE `your-project-id1234.M66_CRRT.TabA9`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M66_CRRT.TabA8`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M66_CRRT.TabA10`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M66_CRRT.TabA9`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M66_CRRT.TabA11`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M66_CRRT.TabA6`   as a
LEFT JOIN `your-project-id1234.M66_CRRT.TabA10`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M66_CRRT.TabA12`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M66_CRRT.TabA8`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M66_CRRT.TabA13`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M66_CRRT.TabA7`  )    ;



CREATE TABLE `your-project-id1234.M66_CRRT.TabA14`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M66_CRRT.TabA9`    as a
LEFT JOIN   `your-project-id1234.M66_CRRT.TabA10`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
