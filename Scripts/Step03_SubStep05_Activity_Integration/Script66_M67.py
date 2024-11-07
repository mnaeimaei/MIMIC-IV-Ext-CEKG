import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M67_DDB` ;
create table `your-project-id1234.M67_DDB.TabA1` as
SELECT * FROM `your-project-id1234.S_Derived1.differential_detailed` ;



create table `your-project-id1234.M67_DDB.TabA2` as
SELECT * FROM  `your-project-id1234.M67_DDB.TabA1` ;
update  `your-project-id1234.M67_DDB.TabA2`
set hadm_id = 0
where hadm_id is null;



ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M67_DDB.TabA2`
SET Activity_Synonym ="DDB"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  Activity STRING ;
UPDATE `your-project-id1234.M67_DDB.TabA2`
SET Activity ="Detailed_differential_blood_count"
WHERE Activity is null ;





ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f1 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f1="specimen_id" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f2 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f2="AbsoluteBasophilCount" 
where f2 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f3 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f3="AbsoluteEosinophilCount" 
where f3 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f4 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f4="AbsoluteLymphocyteCount" 
where f4 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f5 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f5="AbsoluteMonocyteCount" 
where f5 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f6 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f6="AbsoluteNeutrophilCount" 
where f6 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f7 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f7="AtypicalLymphocytes" 
where f7 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f8 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f8="Bands" 
where f8 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f9 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f9="PlateletCount" 
where f9 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f10 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f10="RedBloodCells" 
where f10 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f11 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f11="WhiteBloodCells" 
where f11 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f12 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f12="Basophils" 
where f12 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f13 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f13="Eosinophils" 
where f13 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f14 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f14 ="Lymphocytes" 
where f14 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f15 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f15 ="Monocytes" 
where f15 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f16 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f16 ="Myelocytes" 
where f16 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f17 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f17 ="Neutrophils" 
where f17 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f18 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f18 ="Blasts" 
where f18 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f19 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f19 ="GranulocyteCount" 
where f19 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f20 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f20 ="HypersegmentedNeutrophils" 
where f20 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f21 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f21 ="ImmatureGranulocytes" 
where f21 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f22 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f22 ="LymphocytesPercent" 
where f22 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f23 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f23 ="Metamyelocytes" 
where f23 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f24 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f24 ="Microcytes" 
where f24 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f25 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f25 ="MonocyteCount" 
where f25 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f26 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f26="EosinophilCount" 
where f26 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f27 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f27="NucleatedRedCells" 
where f27 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f28 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f28="ReticulocyteCountAutomated" 
where f28 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f29 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f29="ReticulocyteCountAbsolute" 
where f29 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f30 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f30="WBCCount" 
where f30 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f31 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f31="OtherCells" 
where f31 is null;
#################################################
ALTER TABLE `your-project-id1234.M67_DDB.TabA2`
ADD column  f32 STRING ;
update `your-project-id1234.M67_DDB.TabA2`
set f32="Promyelocytes" 
where f32 is null;
################################################################

create table `your-project-id1234.M67_DDB.TabA3`   as
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(specimen_id AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(AbsoluteBasophilCount AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(AbsoluteEosinophilCount AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(AbsoluteLymphocyteCount AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(AbsoluteMonocyteCount AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(AbsoluteNeutrophilCount AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(AtypicalLymphocytes AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(Bands AS STRING) as  value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(PlateletCount AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(RedBloodCells AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f11 as feature , CAST(WhiteBloodCells AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f12 as feature , CAST(Basophils AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f13 as feature , CAST(Eosinophils AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f14 as feature , CAST(Lymphocytes AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f15 as feature , CAST(Monocytes AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f16 as feature , CAST(Myelocytes AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f17 as feature , CAST(Neutrophils AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f18 as feature , CAST(Blasts AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f19 as feature , CAST(GranulocyteCount AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f20 as feature , CAST(HypersegmentedNeutrophils AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f21 as feature , CAST(ImmatureGranulocytes AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f22 as feature , CAST(LymphocytesPercent AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f23 as feature , CAST(Metamyelocytes AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f24 as feature , CAST(Microcytes AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f25 as feature , CAST(MonocyteCount AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f26 as feature , CAST(EosinophilCount AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f27 as feature , CAST(NucleatedRedCells AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f28 as feature , CAST(ReticulocyteCountAutomated AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f29 as feature , CAST(ReticulocyteCountAbsolute AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f30 as feature , CAST(WBCCount AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f31 as feature , CAST(OtherCells AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`  
union distinct
select subject_id, hadm_id,   charttime as Timestamps, Activity,   Activity_Synonym, f32 as feature , CAST(Promyelocytes AS STRING) as value
from  `your-project-id1234.M67_DDB.TabA2`  ;
#################################################
alter table `your-project-id1234.M67_DDB.TabA3`
add column num INT64;
update`your-project-id1234.M67_DDB.TabA3`
set num=1
where num is null;



   CREATE TABLE `your-project-id1234.M67_DDB.TabA4`  AS

   SELECT
   a.num,a.subject_id, a.hadm_id, a.Timestamps,
   b.RankN,
   a.Activity, a.Activity_Synonym, a.feature, a.value
   FROM `your-project-id1234.M67_DDB.TabA3`  a
   LEFT   JOIN (
   SELECT
   num,subject_id, hadm_id, Timestamps,
   Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
   FROM

   (SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M67_DDB.TabA3`  )  ) b
   ON
   a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;





ALTER TABLE `your-project-id1234.M67_DDB.TabA4`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M67_DDB.TabA4`
SET Activity_Value_ID = concat("ddb",RankN)
WHERE Activity_Value_ID is null ;



CREATE TABLE `your-project-id1234.M67_DDB.TabA5`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234.M67_DDB.TabA4`  ;

CREATE TABLE `your-project-id1234.M67_DDB.TabA6`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234.M67_DDB.TabA4`  ;



ALTER TABLE `your-project-id1234.M67_DDB.TabA6`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M67_DDB.TabA6`
SET Activity_Synonym = "DDB"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M67_DDB.TabA6`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M67_DDB.TabA6`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M67_DDB.TabA7`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M67_DDB.TabA6`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M67_DDB.TabA6`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;



CREATE TABLE `your-project-id1234.M67_DDB.TabA8`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M67_DDB.TabA7`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M67_DDB.TabA9`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M67_DDB.TabA8`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M67_DDB.TabA10`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M67_DDB.TabA5`   as a
LEFT JOIN `your-project-id1234.M67_DDB.TabA9`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M67_DDB.TabA11`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M67_DDB.TabA7`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M67_DDB.TabA12`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M67_DDB.TabA6`  )    ;



CREATE TABLE `your-project-id1234.M67_DDB.TabA13`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M67_DDB.TabA8`    as a
LEFT JOIN   `your-project-id1234.M67_DDB.TabA9`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
