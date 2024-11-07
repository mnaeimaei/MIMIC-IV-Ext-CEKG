import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.I01_Log.TabB16`   AS
        SELECT * FROM (
        SELECT
        a.ID_Distinct_N , a.subject_id , a.hadm_id , a.Timestamps , a.helper, b.hadm_id as prev
        From `your-project-id1234.I01_Log.TabB15`   as a
        LEFT JOIN `your-project-id1234.I01_Log.TabB15`   as b
        ON
        a.ID_Distinct_N=(b.ID_Distinct_N)+1 and a.subject_id=b.subject_id)    
    ; 


        CREATE TABLE  `your-project-id1234.I01_Log.TabB17`   AS
        SELECT * FROM (
        SELECT
        a.ID_Distinct_N , a.subject_id , a.hadm_id ,  a.hadm_id as hadm_id_Final , a.Timestamps , a.helper , a.prev, b.hadm_id as next
        From `your-project-id1234.I01_Log.TabB16`   as a
        LEFT JOIN `your-project-id1234.I01_Log.TabB16`   as b
        ON
        a.ID_Distinct_N=(b.ID_Distinct_N)-1 and a.subject_id=b.subject_id)    
    ; 
    
    
'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
