import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


                CREATE TABLE  `your-project-id1234.I01_Log.TabB17_Rel`   AS
        SELECT distinct * FROM (
        SELECT
        a.ID_unique , a.ID_Distinct , a.hadm_id, b.hadm_id_Final ,
        From `your-project-id1234.I01_Log.TabB13_Rel`   as a
        LEFT JOIN `your-project-id1234.I01_Log.TabB17`   as b
        ON
        a.hadm_id=b.hadm_id )    
    ; 




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
