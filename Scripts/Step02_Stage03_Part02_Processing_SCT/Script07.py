import os
from google.cloud import bigquery

symPath = '../gcKey/SNOMED_CT_Google_Cloud.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''   



CREATE TABLE  `snomed-ct-ml4219.K05_SCT.B_3`   AS
SELECT distinct
a.conceptId,
a.termA,
a.termA_p1,
a.termA_p2,
a.termB,
b.ConceptType

FROM `snomed-ct-ml4219.K05_SCT.B_1` a
left join `snomed-ct-ml4219.K05_SCT.B_2` b
on 
a.conceptId=b.conceptId


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
