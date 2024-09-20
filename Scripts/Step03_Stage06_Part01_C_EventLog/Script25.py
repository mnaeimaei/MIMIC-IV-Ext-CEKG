import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE `your-project-id1234.I01_Log.TabD04`  AS
SELECT 
subject_id, hadm_id, hadm_id_syn
FROM `your-project-id1234.I01_Log.TabD03A` 

union distinct

SELECT 
subject_id, hadm_id,  hadm_id_syn
FROM `your-project-id1234.I01_Log.TabD03B` ;


        

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
