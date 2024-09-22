import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE  `your-project-id1234.I17_DK7.TabA_features_mainEntities`   AS
SELECT distinct * FROM (
SELECT
a.Activity_Value_ID , a.Activity , a.Activity_Synonym , a.featureName , a.featureValue,
b.subject_id , b.hadm_id , b.Timestamps as Timestamp
From `your-project-id1234.I17_DK7.TabA_features`   as a
LEFT JOIN `your-project-id1234.I01_Log.TabE05`   as b
ON
a.Activity_Value_ID=b.Activity_Value_ID )    ; 




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
