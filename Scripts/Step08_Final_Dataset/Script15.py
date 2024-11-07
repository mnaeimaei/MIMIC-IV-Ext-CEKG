import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.N_ClusterReference1`  AS
SELECT distinct 
subject_id	temp_patient_id ,
MC_num as Morbid_num,
hadm_num as Admission_num,
gender,
anchor_age,
dod
FROM `your-project-id1234.O_NonEvents_ClusData.Q07_AdmissionPatientHadm`   ;

CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.N_ClusterReference2`  AS    
SELECT distinct 
subject_id	temp_patient_id ,
hadm_id temp_encounter_id,
ICD10 as ICD10_Code,
ICD10_Root as ICD10_Code_Root,
long_title as ICD10_Code_title,
ICD10_Root_title as ICD10_Code_Root_title,
MC_num  as Morbid_num,
hadm_num as Admission_num,

FROM `your-project-id1234.O_NonEvents_ICD3.icdCM06`; 






'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
