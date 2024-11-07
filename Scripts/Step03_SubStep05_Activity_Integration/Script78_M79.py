import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M79_BGT` ;
create table `your-project-id1234.M79_BGT.TabA1` as
SELECT * FROM `your-project-id1234.N08_BG.bg` ;



update `your-project-id1234.M79_BGT.TabA1`
set hadm_id=0
where hadm_id is null;



ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M79_BGT.TabA1`
SET Activity_Synonym ="BGT"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M79_BGT.TabA1`
SET Activity ="Blood_Gas_Test"
WHERE Activity is null ;



ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f1 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f1="specimen" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f2 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f2="so2" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f3 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f3="po2" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f4 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f4="pco2" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f5 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f5="fio2_chartevents" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f6 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f6="fio2" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f7 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f7="aado2" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f8 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f8="aado2_calc" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f9 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f9="pao2fio2ratio" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f10 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f10="ph" 
where f10 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f11 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f11="baseexcess" 
where f11 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f12 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f12="bicarbonate" 
where f12 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f13 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f13="totalco2" 
where f13 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f14 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f14 ="hematocrit" 
where f14 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f15 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f15 ="hemoglobin" 
where f15 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f16 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f16 ="carboxyhemoglobin" 
where f16 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f17 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f17 ="methemoglobin" 
where f17 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f18 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f18 ="chloride" 
where f18 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f19 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f19 ="calcium" 
where f19 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f20 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f20 ="temperature" 
where f20 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f21 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f21 ="potassium" 
where f21 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f22 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f22 ="sodium" 
where f22 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f23 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f23 ="lactate" 
where f23 is null;
#################################################
ALTER TABLE `your-project-id1234.M79_BGT.TabA1`
ADD column  f24 STRING ;
update `your-project-id1234.M79_BGT.TabA1`
set f24 ="glucose" 
where f24 is null;
################################################################

create table `your-project-id1234.M79_BGT.TabA2`   as
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(specimen AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(so2 AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(po2 AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(pco2 AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(fio2_chartevents AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(fio2 AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(aado2 AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(aado2_calc AS STRING) as  value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(pao2fio2ratio AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(ph AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f11 as feature , CAST(baseexcess AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f12 as feature , CAST(bicarbonate AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f13 as feature , CAST(totalco2 AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f14 as feature , CAST(hematocrit AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f15 as feature , CAST(hemoglobin AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f16 as feature , CAST(carboxyhemoglobin AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f17 as feature , CAST(methemoglobin AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f18 as feature , CAST(chloride AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f19 as feature , CAST(calcium AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f20 as feature , CAST(temperature AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f21 as feature , CAST(potassium AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f22 as feature , CAST(sodium AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f23 as feature , CAST(lactate AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f24 as feature , CAST(glucose AS STRING) as value
from  `your-project-id1234.M79_BGT.TabA1` ;
#################################################
alter table `your-project-id1234.M79_BGT.TabA2`
add column num INT64;
update`your-project-id1234.M79_BGT.TabA2`
set num=1
where num is null;



   CREATE TABLE `your-project-id1234.M79_BGT.TabA3`  AS

   SELECT
   a.num,a.subject_id, a.hadm_id, a.Timestamps,
   b.RankN,
   a.Activity, a.Activity_Synonym, a.feature, a.value
   FROM `your-project-id1234.M79_BGT.TabA2`  a
   LEFT   JOIN (
   SELECT
   num,subject_id, hadm_id, Timestamps,
   Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
   FROM

   (SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M79_BGT.TabA2`  )  ) b
   ON
   a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;








ALTER TABLE `your-project-id1234.M79_BGT.TabA3`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M79_BGT.TabA3`
SET Activity_Value_ID = concat("bgt",RankN)
WHERE Activity_Value_ID is null ;




CREATE TABLE `your-project-id1234.M79_BGT.TabA4`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M79_BGT.TabA3`  ;

CREATE TABLE `your-project-id1234.M79_BGT.TabA5`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M79_BGT.TabA3`  ;



ALTER TABLE `your-project-id1234.M79_BGT.TabA5`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M79_BGT.TabA5`
SET Activity_Synonym = "BGT"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M79_BGT.TabA5`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M79_BGT.TabA5`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M79_BGT.TabA6`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M79_BGT.TabA5`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M79_BGT.TabA5`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




CREATE TABLE `your-project-id1234.M79_BGT.TabA7`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M79_BGT.TabA6`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M79_BGT.TabA8`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M79_BGT.TabA7`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M79_BGT.TabA9`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M79_BGT.TabA4`   as a
LEFT JOIN `your-project-id1234.M79_BGT.TabA8`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M79_BGT.TabA10`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M79_BGT.TabA6`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M79_BGT.TabA11`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M79_BGT.TabA5`  )    ;



CREATE TABLE `your-project-id1234.M79_BGT.TabA12`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M79_BGT.TabA7`    as a
LEFT JOIN   `your-project-id1234.M79_BGT.TabA8`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
