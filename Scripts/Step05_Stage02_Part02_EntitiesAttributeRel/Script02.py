import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_Patient_Gender`  AS
SELECT distinct
Entity Entity_Origin1
, subject_id Entity_ID1
, Origin Entity_Origin2
, ID Entity_ID2
, subject_id,
hadm_id

FROM `your-project-id1234.O_NonEvents_ClusData.Q01_PatientHadmGender` ;




CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_Admission_Age`  AS
SELECT distinct
Entity Entity_Origin1
, hadm_id Entity_ID1
, Origin Entity_Origin2
, ID Entity_ID2
, subject_id,
hadm_id
FROM `your-project-id1234.O_NonEvents_ClusData.Q01_ageX3` ;

########################################################################################

CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_adm_Dis`  AS
SELECT distinct *
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM19_adm_Dis_final` ;


CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_adm_mm`  AS
SELECT distinct *
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM19_adm_mm_final` ;

CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_mm_new`  AS
SELECT distinct *
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM19_mm_new_final` ;

CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_mm_treat`  AS
SELECT distinct *
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM19_mm_treat_final` ;

CREATE TABLE `your-project-id1234.I03_EntityRel.TabA_mm_unt`  AS
SELECT distinct *
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM19_mm_unt_final` ;








'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
