import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M82_FGCS` ;
create table `your-project-id1234.M82_FGCS.TabA1` as
SELECT *  FROM `your-project-id1234.N09_GCS.first_day_gcs_time` ;



CREATE TABLE  `your-project-id1234.M82_FGCS.TabA2`   AS
SELECT distinct * FROM (
SELECT
a.subject_id , b.hadm_id, a.stay_id , a.Timestamps , a.gcs_min , a.gcs_motor , a.gcs_verbal , a.gcs_eyes , a.gcs_unable,

From `your-project-id1234.M82_FGCS.TabA1`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SHI`   as b
ON
a.subject_id=b.subject_id AND a.stay_id=b.stay_id
AND  a.Timestamps>=b.min AND a.Timestamps<=b.max
)
;



CREATE TABLE  `your-project-id1234.M82_FGCS.TabA3`   AS
SELECT distinct   
subject_id, hadm_id, Timestamps, gcs_min, gcs_motor, gcs_verbal, gcs_eyes, gcs_unable  
From `your-project-id1234.M82_FGCS.TabA2` ;



ALTER TABLE `your-project-id1234.M82_FGCS.TabA3`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M82_FGCS.TabA3`
SET Activity_Synonym ="FGCS"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M82_FGCS.TabA3`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M82_FGCS.TabA3`
SET Activity ="First_Day_GCS_Assessment"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M82_FGCS.TabA3`
ADD column  f1 STRING ;
update `your-project-id1234.M82_FGCS.TabA3`
set f1="gcs_min" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M82_FGCS.TabA3`
ADD column  f2 STRING ;
update `your-project-id1234.M82_FGCS.TabA3`
set f2="gcs_motor" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M82_FGCS.TabA3`
ADD column  f3 STRING ;
update `your-project-id1234.M82_FGCS.TabA3`
set f3="gcs_verbal" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M82_FGCS.TabA3`
ADD column  f4 STRING ;
update `your-project-id1234.M82_FGCS.TabA3`
set f4="gcs_eyes" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M82_FGCS.TabA3`
ADD column  f5 STRING ;
update `your-project-id1234.M82_FGCS.TabA3`
set f5="gcs_unable" 
where f5 is null;
################################################################

create table `your-project-id1234.M82_FGCS.TabA4`   as
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(gcs_min AS STRING) as value
from  `your-project-id1234.M82_FGCS.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(gcs_motor AS STRING) as value
from  `your-project-id1234.M82_FGCS.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(gcs_verbal AS STRING) as value
from  `your-project-id1234.M82_FGCS.TabA3`  
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(gcs_eyes AS STRING) as value
from  `your-project-id1234.M82_FGCS.TabA3`
union distinct
select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(gcs_unable AS STRING) as value
from  `your-project-id1234.M82_FGCS.TabA3` ;
#################################################
alter table `your-project-id1234.M82_FGCS.TabA4`
add column num INT64;
update`your-project-id1234.M82_FGCS.TabA4`
set num=1
where num is null;



CREATE TABLE `your-project-id1234.M82_FGCS.TabA5`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M82_FGCS.TabA4`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M82_FGCS.TabA4`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;





ALTER TABLE `your-project-id1234.M82_FGCS.TabA5`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M82_FGCS.TabA5`
SET Activity_Value_ID = concat("fgcs",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M82_FGCS.TabA6`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M82_FGCS.TabA5`  ;

CREATE TABLE `your-project-id1234.M82_FGCS.TabA7`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M82_FGCS.TabA5`  ;




ALTER TABLE `your-project-id1234.M82_FGCS.TabA7`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M82_FGCS.TabA7`
SET Activity_Synonym = "FGCS"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M82_FGCS.TabA7`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M82_FGCS.TabA7`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M82_FGCS.TabA8`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M82_FGCS.TabA7`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M82_FGCS.TabA7`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M82_FGCS.TabA9`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M82_FGCS.TabA8`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M82_FGCS.TabA10`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M82_FGCS.TabA9`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M82_FGCS.TabA11`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M82_FGCS.TabA6`   as a
LEFT JOIN `your-project-id1234.M82_FGCS.TabA10`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M82_FGCS.TabA12`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M82_FGCS.TabA8`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M82_FGCS.TabA13`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M82_FGCS.TabA7`  )    ;



CREATE TABLE `your-project-id1234.M82_FGCS.TabA14`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M82_FGCS.TabA9`    as a
LEFT JOIN   `your-project-id1234.M82_FGCS.TabA10`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
