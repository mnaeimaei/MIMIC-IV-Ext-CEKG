import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        CREATE TABLE  `your-project-id1234.I01_Log.TabC01`   AS
        SELECT * FROM (
        SELECT
        a.subject_id , b.hadm_id_Final as hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID , a.Activity_Properties_ID_aggregation,
        From `your-project-id1234.I01_Log.TabB01`   as a
        LEFT JOIN `your-project-id1234.I01_Log.TabB17_Rel`   as b
        ON
        a.ID_unique=b.ID_unique )    
    ; 




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
