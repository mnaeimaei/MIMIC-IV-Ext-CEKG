import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.C_EntitiesAttributes`  AS
SELECT 
Origin	,
ID,
Name,
Value,
Category,
subject_id	temp_patient_id ,
hadm_id temp_encounter_id

FROM `your-project-id1234.I02_otherEntities.TabB`  ;
'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
