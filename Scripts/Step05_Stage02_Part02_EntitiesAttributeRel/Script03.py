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
subject_id,
STRING_AGG(hadm_id,"," ORDER BY hadm_id)  as hadm_id_agg
FROM `your-project-id1234.I01_Log.TabC01`  
GROUP BY subject_id;

CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm2`  AS
SELECT distinct 
concat("Patient") as Entity_Origin1  ,
subject_id Entity_ID1, 
concat("Admission") as Entity_Origin2  ,
hadm_id_agg Entity_ID2, 
FROM `your-project-id1234.I03_EntityRel.TabA_patientAdm1` ;

CREATE TABLE  `your-project-id1234.I03_EntityRel.TabA_patientAdm`   AS
SELECT * FROM (
SELECT
a.Entity_Origin1 , a.Entity_ID1 , a.Entity_Origin2 , a.Entity_ID2, a.Entity_ID1 as subject_id,
b.hadm_id 
From `your-project-id1234.I03_EntityRel.TabA_patientAdm2`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SH`   as b
ON         a.Entity_ID1=b.subject_id )        ; 



                
drop TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm1`  ;


drop TABLE `your-project-id1234.I03_EntityRel.TabA_patientAdm2`  ;

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
