import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE `your-project-id1234.I04_actPro.TabTemp`  AS
SELECT
DISTINCT element AS Activity_Properties_ID, subject_id, hadm_id, 
FROM
`your-project-id1234.I01_Log.TabC01`,
UNNEST(SPLIT(Activity_Properties_ID_aggregation, ',')) AS element
ORDER BY
Activity_Properties_ID;

CREATE TABLE  `your-project-id1234.I04_actPro.TabB1`   AS
SELECT distinct * FROM (
SELECT
a.Activity_Properties_ID , a.Activity , a.Activity_Synonym , a.featureName , a.featureValue, b.subject_id , b.hadm_id,
From `your-project-id1234.I04_actPro.TabA1`   as a
LEFT JOIN `your-project-id1234.I04_actPro.TabTemp`   as b
ON
a.Activity_Properties_ID=b.Activity_Properties_ID )    ; 

drop TABLE `your-project-id1234.I04_actPro.TabTemp` ;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
