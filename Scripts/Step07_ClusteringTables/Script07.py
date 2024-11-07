import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.O_NonEvents_ClusData.Q06_AdmissionPatient`   AS
        SELECT distinct
        a.subject_id, 
        a.gender_Int as gender,
        b.age as anchor_age,
        a.Died as dod
        From `your-project-id1234.O_NonEvents_ClusData.Q05_AdmissionPatient`   as a
        LEFT JOIN `your-project-id1234.O_NonEvents_ClusData.Q01_ageFinal`   as b
        ON
        a.subject_id=b.subject_id 
        ;

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
