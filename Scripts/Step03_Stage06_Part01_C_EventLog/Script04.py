import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            
CREATE TABLE `your-project-id1234.I01_Log.TabB05`  AS

        SELECT * FROM (
        SELECT
        a.ID_Distinct , a.subject_id , a.hadm_id , a.Timestamps,
        b.hadm_id as previous
        From `your-project-id1234.I01_Log.TabB04`   as a
        LEFT JOIN `your-project-id1234.I01_Log.TabB04`   as b
        ON
        a.ID_Distinct=(b.ID_Distinct)+1 and a.subject_id=b.subject_id)    



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
