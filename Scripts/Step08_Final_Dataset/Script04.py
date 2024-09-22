import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.D_EntitiesAttributeRel`  AS
SELECT 
        

Entity_Origin1 as Origin1,
Entity_ID1 as ID1,
Entity_Origin2 as Origin2,
Entity_ID2 as ID2,
subject_id	temp_patient_id ,
hadm_id temp_encounter_id
FROM `your-project-id1234.I03_EntityRel.TabB`  ;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
