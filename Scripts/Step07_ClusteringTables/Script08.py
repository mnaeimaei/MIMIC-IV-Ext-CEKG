import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.O_NonEvents_ClusData.Q07_AdmissionPatientHadm`   AS
        SELECT distinct
        a.subject_id, 
        a.MC_num,
        a.hadm_num,
        b.gender,
        b.anchor_age,
        b.dod
        FROM `your-project-id1234.O_NonEvents_ClusData.Q01_icd` a
        left join   `your-project-id1234.O_NonEvents_ClusData.Q06_AdmissionPatient` b
        on 
        a. subject_id = b.subject_id;
        
        



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
