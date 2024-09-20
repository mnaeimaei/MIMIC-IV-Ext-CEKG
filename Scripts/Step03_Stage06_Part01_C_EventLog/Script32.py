import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create table `your-project-id1234.I01_Log.TabE05`   as
SELECT
eventID,
subject_id,
hadm_id,
hadm_id_syn,
Timestamps,
Activity,
Activity_Synonym,
Activity_Value_ID,
Activity_Properties_ID_aggregation,
Ent1,
En1_ID,
Ent2,
CAST(En2_ID AS INT64) AS En2_ID,
Ent3,
CAST(En3_ID AS INT64) AS En3_ID,
Ent4,
CAST(En4_ID AS INT64) AS En4_ID,
Ent5,
CAST(En5_ID AS INT64) AS En5_ID,
FROM  `your-project-id1234.I01_Log.TabE04` ;





'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
