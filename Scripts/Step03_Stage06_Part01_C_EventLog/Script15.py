import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


UPDATE `your-project-id1234.I01_Log.TabB13`  
SET helper ="2"
where helper="0" and  hadm_prev is null;

UPDATE `your-project-id1234.I01_Log.TabB13`  
SET helper ="2"
where helper="0" and  hadm_id<>hadm_prev;

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
