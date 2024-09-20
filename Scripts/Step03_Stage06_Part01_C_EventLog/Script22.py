import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



CREATE TABLE  `your-project-id1234.I01_Log.TabD01A`   AS
SELECT distinct
subject_id, hadm_id, max(Timestamps) maxT
FROM `your-project-id1234.I01_Log.TabC01` 
where hadm_id not like "O%"
group by subject_id, hadm_id;

CREATE TABLE  `your-project-id1234.I01_Log.TabD01B`   AS
SELECT distinct
subject_id, hadm_id, max(Timestamps) maxT
FROM `your-project-id1234.I01_Log.TabC01` 
where hadm_id like "O%"
group by subject_id, hadm_id;
        
        

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
