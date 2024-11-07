import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

create schema `your-project-id1234.M08_MBS` ;
create table `your-project-id1234.M08_MBS.TabA1` as
SELECT * FROM `your-project-id1234.S_Microbiology.1_Microbiology` ;
create table `your-project-id1234.M08_MBS.TabA2` as
SELECT * FROM `your-project-id1234.M08_MBS.TabA1` ;



update  `your-project-id1234.M08_MBS.TabA2`
set hadm_id = 0
where hadm_id is null	;



#############################################################################
update  `your-project-id1234.M08_MBS.TabA2`
set spec_type_desc = "NULL"
where spec_type_desc=""	;
#############################################################################
UPDATE `your-project-id1234.M08_MBS.TabA2`
SET  org_name  = "no organism grew"
WHERE org_name is null ;
#############################################################################
UPDATE `your-project-id1234.M08_MBS.TabA2`
SET  ab_name  = "no need antibiotic"
WHERE org_name  = "no organism grew";

UPDATE `your-project-id1234.M08_MBS.TabA2`
SET  ab_name  = "no antibiotic"
WHERE org_name <> "no organism grew" and ab_name is null;

#############################################################################
UPDATE `your-project-id1234.M08_MBS.TabA2`
SET  interpretation  = "no need interpretation"
WHERE org_name  = "no organism grew";

UPDATE `your-project-id1234.M08_MBS.TabA2`
SET  interpretation  = "no interpretation"
WHERE org_name <> "no organism grew"  and interpretation is null;
#############################################################################



create table `your-project-id1234.M08_MBS.TabA3` as
SELECT
subject_id, hadm_id, chartdate, charttime,storedate, storetime,
test_name, spec_type_desc, org_name, ab_name,interpretation
FROM `your-project-id1234.M08_MBS.TabA2` ;



create table `your-project-id1234.M08_MBS.TabA4` as
select distinct * from
(SELECT
subject_id, hadm_id, charttime as Timestamp, test_name, spec_type_desc
FROM `your-project-id1234.M08_MBS.TabA3` 
where charttime is not null 
union all
select
subject_id, hadm_id, chartdate as Timestamp, test_name, spec_type_desc
FROM `your-project-id1234.M08_MBS.TabA3` 
where charttime is  null );



ALTER   TABLE `your-project-id1234.M08_MBS.TabA4`
ADD    COLUMN  Activity STRING ;
UPDATE `your-project-id1234.M08_MBS.TabA4`
SET  Activity  = "Mirobiology_Blood_Sample"
WHERE Activity is null ;
############################################################
ALTER   TABLE `your-project-id1234.M08_MBS.TabA4`
ADD    COLUMN  Activity_Synonym STRING ;
UPDATE `your-project-id1234.M08_MBS.TabA4`
SET  Activity_Synonym  = "MBS"
WHERE Activity_Synonym is null ;



ALTER TABLE `your-project-id1234.M08_MBS.TabA4`
ADD column  f1 STRING ;
update `your-project-id1234.M08_MBS.TabA4`
set f1="test_name" 
where f1 is null;
#################################################
ALTER TABLE `your-project-id1234.M08_MBS.TabA4`
ADD column  f2 STRING ;
update `your-project-id1234.M08_MBS.TabA4`
set f2="spec_type_desc" 
where f2 is null;
#################################################

create table `your-project-id1234.M08_MBS.TabA5`   as
select subject_id, hadm_id, Timestamp as Timestamps, Activity, Activity_Synonym, f1 as feature , test_name as value
from  `your-project-id1234.M08_MBS.TabA4`  
union distinct
select subject_id, hadm_id, Timestamp as Timestamps, Activity, Activity_Synonym, f2 as feature , spec_type_desc as value
from  `your-project-id1234.M08_MBS.TabA4`  ;
#################################################
alter table `your-project-id1234.M08_MBS.TabA5`
add column num INT64;
update`your-project-id1234.M08_MBS.TabA5`
set num=1
where num is null;




CREATE TABLE `your-project-id1234.M08_MBS.TabA6`  AS

SELECT
a.num,a.subject_id, a.hadm_id, a.Timestamps,
b.RankN,
a.Activity, a.Activity_Synonym, a.feature, a.value
FROM `your-project-id1234.M08_MBS.TabA5`  a
LEFT   JOIN (
SELECT
num,subject_id, hadm_id, Timestamps,
Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
FROM

(SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M08_MBS.TabA5`  )  ) b
ON
a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;





ALTER TABLE `your-project-id1234.M08_MBS.TabA6`
ADD column  Activity_Value_ID STRING ;

UPDATE `your-project-id1234.M08_MBS.TabA6`
SET Activity_Value_ID = concat("mbs",RankN)
WHERE Activity_Value_ID is null ;





CREATE TABLE `your-project-id1234.M08_MBS.TabA7`  AS
SELECT
subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
FROM `your-project-id1234..M08_MBS.TabA6`  ;

CREATE TABLE `your-project-id1234.M08_MBS.TabA8`  AS
SELECT
Activity_Value_ID, Activity, feature as featureName, value as featureValue
FROM `your-project-id1234..M08_MBS.TabA6`  ;



ALTER TABLE `your-project-id1234.M08_MBS.TabA8`
ADD column  Activity_Synonym STRING ;

UPDATE `your-project-id1234.M08_MBS.TabA8`
SET Activity_Synonym = "MBS"
WHERE Activity_Synonym is null ;
ALTER TABLE `your-project-id1234.M08_MBS.TabA8`
ADD column  num INT64 ;

UPDATE `your-project-id1234.M08_MBS.TabA8`
SET num = 1
WHERE num is null ;



CREATE TABLE `your-project-id1234.M08_MBS.TabA9`  AS

SELECT
a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
b.RankN,
a.Activity_Value_ID
FROM `your-project-id1234.M08_MBS.TabA8`  a
LEFT   JOIN (
SELECT
num,Activity, Activity_Synonym, featureName, featureValue,
Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
FROM

(SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M08_MBS.TabA8`  )  ) b
ON
a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;



CREATE TABLE `your-project-id1234.M08_MBS.TabA10`  AS
SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
FROM `your-project-id1234.M08_MBS.TabA9`
where RankN is not null
order by Activity_Value_ID;



CREATE TABLE `your-project-id1234.M08_MBS.TabA11`  AS
SELECT distinct
Activity_Value_ID,
STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
FROM `your-project-id1234.M08_MBS.TabA10`
GROUP BY Activity_Value_ID;



CREATE TABLE `your-project-id1234.M08_MBS.TabA12`  AS
SELECT distinct * FROM (
SELECT distinct
a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
b.Activity_Properties_ID_aggregation
From `your-project-id1234.M08_MBS.TabA7`   as a
LEFT JOIN `your-project-id1234.M08_MBS.TabA11`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;


CREATE TABLE `your-project-id1234.M08_MBS.TabA13`  AS
SELECT distinct  * FROM (
SELECT
concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
From `your-project-id1234.M08_MBS.TabA9`
where RankN is not null)    ;
CREATE TABLE `your-project-id1234.M08_MBS.TabA14`  AS
SELECT distinct  * FROM (
SELECT
Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
From  `your-project-id1234.M08_MBS.TabA8`  )    ;



CREATE TABLE `your-project-id1234.M08_MBS.TabA15`  AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
From  `your-project-id1234.M08_MBS.TabA10`    as a
LEFT JOIN   `your-project-id1234.M08_MBS.TabA11`    as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )        ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
