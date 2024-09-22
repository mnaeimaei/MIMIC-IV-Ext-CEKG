import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.I03_EntityRel.TabB`  AS

SELECT  
Entity_Origin1,  CAST(Entity_ID1 AS STRING) AS Entity_ID1, Entity_Origin2, CAST(Entity_ID2 AS STRING) AS Entity_ID2, subject_id , CAST(hadm_id AS STRING) AS hadm_id  
FROM `your-project-id1234.I03_EntityRel.TabA_Admission_Age` 

union all

SELECT  
Entity_Origin1, CAST(Entity_ID1 AS STRING) AS Entity_ID1, Entity_Origin2, CAST(Entity_ID2 AS STRING) AS Entity_ID2, subject_id , CAST(hadm_id AS STRING) AS hadm_id  
FROM `your-project-id1234.I03_EntityRel.TabA_Patient_Gender` 

union all

SELECT  
Entity_Origin1, CAST(Entity_ID1 AS STRING) AS Entity_ID1, Entity_Origin2, CAST(Entity_ID2 AS STRING) AS Entity_ID2, subject_id , CAST(hadm_id AS STRING) AS hadm_id  
FROM `your-project-id1234.I03_EntityRel.TabA_adm_Dis` 

union all

SELECT  
Entity_Origin1, CAST(Entity_ID1 AS STRING) AS Entity_ID1, Entity_Origin2, CAST(Entity_ID2 AS STRING) AS Entity_ID2, subject_id , CAST(hadm_id AS STRING) AS hadm_id  
FROM `your-project-id1234.I03_EntityRel.TabA_adm_mm` 

union all

SELECT  
Entity_Origin1, CAST(Entity_ID1 AS STRING) AS Entity_ID1, Entity_Origin2, CAST(Entity_ID2 AS STRING) AS Entity_ID2, subject_id , CAST(hadm_id AS STRING) AS hadm_id  
FROM `your-project-id1234.I03_EntityRel.TabA_mm_new` 

union all

SELECT  
Entity_Origin1, CAST(Entity_ID1 AS STRING) AS Entity_ID1, Entity_Origin2, CAST(Entity_ID2 AS STRING) AS Entity_ID2, subject_id , CAST(hadm_id AS STRING) AS hadm_id  
FROM `your-project-id1234.I03_EntityRel.TabA_mm_treat` 

union all

SELECT  
Entity_Origin1, CAST(Entity_ID1 AS STRING) AS Entity_ID1, Entity_Origin2, CAST(Entity_ID2 AS STRING) AS Entity_ID2, subject_id , CAST(hadm_id AS STRING) AS hadm_id 
FROM `your-project-id1234.I03_EntityRel.TabA_mm_unt` 

union all

SELECT  
Entity_Origin1, CAST(Entity_ID1 AS STRING) AS Entity_ID1, Entity_Origin2, CAST(Entity_ID2 AS STRING) AS Entity_ID2, subject_id , hadm_id 
FROM `your-project-id1234.I03_EntityRel.TabA_patientAdm` ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)