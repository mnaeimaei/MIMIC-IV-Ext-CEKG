import os
from google.cloud import bigquery

symPath = '../gcKey/SNOMED_CT_Google_Cloud.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            
CREATE TABLE  `snomed-ct-ml4219.K02_SCT.ICD_SNOMED`   AS
SELECT distinct * FROM `snomed-ct-ml4219.K02_SCT.CONCEPT_RELATIONSHIP3` 
where vocabulary_id_1="ICD10CM" or vocabulary_id_1="SNOMED" ;

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
