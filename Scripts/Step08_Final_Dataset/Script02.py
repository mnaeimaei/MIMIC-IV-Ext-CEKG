import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.B_EventLog`  AS
SELECT 
eventID as Event_ID,
Timestamps as Timestamp,
Activity ,
Activity_Synonym,
Activity_Properties_ID_aggregation as Activity_Attributes_ID,
Activity_Value_ID as Activity_Instance_ID,

Ent1 as Entity1_origin,
En1_ID as Entity1_ID,

Ent2 as Entity2_origin,
En2_ID as Entity2_ID,

Ent3 as Entity3_origin,
En3_ID as Entity3_ID,

Ent4 as Entity4_origin,
En4_ID as Entity4_ID,

Ent5 as Entity5_origin,
En5_ID as Entity5_ID,

FROM `your-project-id1234.I01_Log.TabE05`  ;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
