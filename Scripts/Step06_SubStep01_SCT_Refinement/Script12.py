import os
from google.cloud import bigquery

symPath = '../gcKey/SNOMED_CT_Google_Cloud.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

###################################
################################### Go to SQL \ Query 10_SCT
###################################
# Perform a query.
query1 = f'''            


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperF`   AS 
SELECT s0,  STRING_AGG(p0 ORDER BY p0) as P0
FROM  `snomed-ct-ml4219.K06_SCT.PaperE` 
GROUP BY s0




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
