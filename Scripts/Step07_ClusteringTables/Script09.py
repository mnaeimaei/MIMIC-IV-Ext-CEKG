import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        
        
CREATE TABLE  `your-project-id1234.O_NonEvents_ClusData.Q07_DisorderPatientHadm`   AS
SELECT distinct * FROM `your-project-id1234.O_NonEvents_ICD3.icdCM06`;  


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
