import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create table `your-project-id1234.I01_Log.TabB09`   as
Select ID_Distinct, subject_id, hadm_id, Timestamps, previous as zeroFinder, hadm_N
from `your-project-id1234.I01_Log.TabB08_A`  
union distinct
Select ID_Distinct, subject_id, hadm_id, Timestamps, previous as zeroFinder, previous as hadm_N
from `your-project-id1234.I01_Log.TabB06_B`  


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
