import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm1`  AS
SELECT distinct
En1_ID as Entity_ID1,
STRING_AGG(CAST(En2_ID AS STRING),"," ORDER BY En2_ID)  as Entity_ID2,
concat("Patient") as Entity_Origin1  ,
concat("Admission") as Entity_Origin2  ,
FROM (select distinct En1_ID, En2_ID from `your-project-id1234.I01_Log.TabE05`  where En2_ID is not null)
GROUP BY En1_ID;



CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm2`  AS
SELECT distinct
En1_ID as Entity_ID1,
STRING_AGG(CAST(En3_ID AS STRING),"," ORDER BY En3_ID)  as Entity_ID2,
concat("Patient") as Entity_Origin1  ,
concat("Outpatient") as Entity_Origin2  ,
FROM (select distinct En1_ID, En3_ID from `your-project-id1234.I01_Log.TabE05`  where En3_ID is not null)
GROUP BY En1_ID;



CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm3`  AS
SELECT distinct
En2_ID as Entity_ID1,
STRING_AGG(CAST(En4_ID AS STRING),"," ORDER BY En4_ID)  as Entity_ID2,
concat("Admission") as Entity_Origin1  ,
concat("Admission_Sequence") as Entity_Origin2  ,
FROM (select distinct En2_ID, En4_ID from `your-project-id1234.I01_Log.TabE05`  where En2_ID is not null)
GROUP BY En2_ID;


CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm4`  AS
SELECT distinct
En3_ID as Entity_ID1,
STRING_AGG(CAST(En5_ID AS STRING),"," ORDER BY En5_ID)  as Entity_ID2,
concat("Outpatient") as Entity_Origin1  ,
concat("Outpatient_Sequence") as Entity_Origin2  ,
FROM (select distinct En3_ID, En5_ID from `your-project-id1234.I01_Log.TabE05`  where En3_ID is not null)
GROUP BY En3_ID;



###################################################


CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm5`  AS
SELECT distinct
a.Entity_Origin1 ,
a.Entity_ID1 , 
a.Entity_Origin2 , 
a.Entity_ID2,
b.subject_id,
b.hadm_id
FROM `your-project-id1234.I03_EntityRel.TabA_patientAdm1` a
left join `your-project-id1234.I01_Log.TabE05` b
on a.Entity_ID1=b.subject_id
where b.En2_ID is not null;


CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm6`  AS
SELECT distinct
a.Entity_Origin1 ,
a.Entity_ID1 , 
a.Entity_Origin2 , 
a.Entity_ID2,
b.subject_id,
b.hadm_id
FROM `your-project-id1234.I03_EntityRel.TabA_patientAdm2` a
left join `your-project-id1234.I01_Log.TabE05` b
on a.Entity_ID1=b.subject_id 
where b.En3_ID is not null;



CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm7`  AS
SELECT distinct
a.Entity_Origin1 ,
a.Entity_ID1 , 
a.Entity_Origin2 , 
a.Entity_ID2,
b.subject_id,
b.hadm_id
FROM `your-project-id1234.I03_EntityRel.TabA_patientAdm3` a
left join `your-project-id1234.I01_Log.TabE05` b
on a.Entity_ID1=b.En2_ID
;



CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm8`  AS
SELECT distinct
a.Entity_Origin1 ,
a.Entity_ID1 , 
a.Entity_Origin2 , 
a.Entity_ID2,
b.subject_id,
b.hadm_id
FROM `your-project-id1234.I03_EntityRel.TabA_patientAdm4` a
left join `your-project-id1234.I01_Log.TabE05` b
on a.Entity_ID1=b.En3_ID
;

#######################################


CREATE TABLE  `your-project-id1234.I03_EntityRel.TabA_patientAdm`   AS
SELECT distinct * FROM (
SELECT Entity_Origin1 , Entity_ID1 , Entity_Origin2 , Entity_ID2, subject_id, hadm_id From `your-project-id1234.I03_EntityRel.TabA_patientAdm5`  
union all
SELECT Entity_Origin1 , Entity_ID1 , Entity_Origin2 , Entity_ID2, subject_id, hadm_id From `your-project-id1234.I03_EntityRel.TabA_patientAdm6`  
union all
SELECT Entity_Origin1 , Entity_ID1 , Entity_Origin2 , Entity_ID2, subject_id, hadm_id From `your-project-id1234.I03_EntityRel.TabA_patientAdm7`  
union all
SELECT Entity_Origin1 , Entity_ID1 , Entity_Origin2 , Entity_ID2, subject_id, hadm_id From `your-project-id1234.I03_EntityRel.TabA_patientAdm8`  );




                
drop TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm1`  ;
drop TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm2`  ;
drop TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm3`  ;
drop TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm4`  ;
drop TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm5`  ;
drop TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm6`  ;
drop TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm7`  ;
drop TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm8`  ;

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
