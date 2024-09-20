import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

UPDATE `your-project-id1234.I01_Log.TabB17`  
SET prev ="X"
WHERE prev is null ;

UPDATE `your-project-id1234.I01_Log.TabB17`  
SET next ="Y"
WHERE next is null ;


UPDATE `your-project-id1234.I01_Log.TabB17`  
SET hadm_id_Final =prev
WHERE prev= next and helper="2";





'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
