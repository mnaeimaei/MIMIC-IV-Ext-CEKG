import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.N_ClusteringMethod1`  AS
SELECT distinct 
subject_id as Entity1_ID,
MC_num as Morbid_num,
hadm_num as Admission_num,
gender,
anchor_age,
dod
FROM `your-project-id1234.O_NonEvents_ClusData.Q07_AdmissionPatientHadm`   ;

CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.N_ClusteringMethod2`  AS    
SELECT distinct 
subject_id as Entity1_ID,
hadm_id as Entity2_ID, 
ICD10 as ICD10_Code,
ICD10_Root as ICD10_Code_Root,
long_title as ICD10_Code_title,
ICD10_Root_title as ICD10_Code_Root_title,
MC_num  as Morbid_num,
hadm_num as Admission_num,
FROM `your-project-id1234.O_NonEvents_ClusData.Q07_DisorderPatientHadm`; 





'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
