import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

      CREATE TABLE `your-project-id1234.O_NonEvents_ClusData.Q01_PatientGender`  AS
      SELECT distinct subject_id, gender, concat("gender") origin, concat("absolute") Category 
FROM `your-project-id1234.O_NonEvents_ClusData.Q01_Patient` ;

alter table  `your-project-id1234.O_NonEvents_ClusData.Q01_PatientGender`  
add column ID INT64;

update  `your-project-id1234.O_NonEvents_ClusData.Q01_PatientGender`  
set ID=1
where gender="M";

update  `your-project-id1234.O_NonEvents_ClusData.Q01_PatientGender`  
set ID=2
where gender="F";

update  `your-project-id1234.O_NonEvents_ClusData.Q01_PatientGender`  
set gender="male"
where gender="M";

update  `your-project-id1234.O_NonEvents_ClusData.Q01_PatientGender`  
set gender="female"
where gender="F";

alter table  `your-project-id1234.O_NonEvents_ClusData.Q01_PatientGender`  
add column Name string;

update  `your-project-id1234.O_NonEvents_ClusData.Q01_PatientGender`  
set Name=concat(origin,ID)
where Name is null;

alter table  `your-project-id1234.O_NonEvents_ClusData.Q01_PatientGender`  
add column Entity string;

update  `your-project-id1234.O_NonEvents_ClusData.Q01_PatientGender`  
set Entity="Patient"
where Entity is null;


#####################################################################################

CREATE TABLE  `your-project-id1234.O_NonEvents_ClusData.Q01_PatientHadmGender`   AS
SELECT * FROM (
SELECT
a.subject_id , a.gender , a.origin , a.Category , a.ID , a.Name , a.Entity,
b.hadm_id 
From `your-project-id1234.O_NonEvents_ClusData.Q01_PatientGender`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SH`   as b
ON
a.subject_id=b.subject_id )    ; 





        

##########################################################################

      CREATE TABLE `your-project-id1234.O_NonEvents_ClusData.Q01_ageFinal`  AS
      SELECT 
subject_id, hadm_id, age

FROM `your-project-id1234.O_NonEvents_ClusData.Q01_age1` 
union distinct
SELECT 
subject_id, hadm_id, age
FROM `your-project-id1234.O_NonEvents_ClusData.Q01_age2` ;

##########################################################################


##########################################################################
      CREATE TABLE `your-project-id1234.O_NonEvents_ClusData.Q01_ageX1`  AS
      SELECT *  
FROM `your-project-id1234.O_NonEvents_ClusData.Q01_ageFinal` ;

alter table  `your-project-id1234.O_NonEvents_ClusData.Q01_ageX1`  
add column num INT64;

update  `your-project-id1234.O_NonEvents_ClusData.Q01_ageX1`  
set num=1
where num is null;

CREATE TABLE `your-project-id1234.O_NonEvents_ClusData.Q01_ageX2`  AS
SELECT
a.subject_id, a.hadm_id, a.age, b.RankN as ID
FROM `your-project-id1234.O_NonEvents_ClusData.Q01_ageX1`  a
LEFT   JOIN (
SELECT  
num,age,
Row_number() over (partition by  num order by age asc) as RankN
FROM
(SELECT   DISTINCT num,age  FROM `your-project-id1234.O_NonEvents_ClusData.Q01_ageX1`  )  ) b
ON
a.num = b.num AND a.age = b.age;

CREATE TABLE `your-project-id1234.O_NonEvents_ClusData.Q01_ageX3`  AS
SELECT distinct 
subject_id
, hadm_id
, concat("Admission") Entity
, concat("age") Origin
, ID
,concat("age",ID) Name
, age Value
, concat("Absolute") as Category
FROM `your-project-id1234.O_NonEvents_ClusData.Q01_ageX2` ;
        
##########################################################################


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
