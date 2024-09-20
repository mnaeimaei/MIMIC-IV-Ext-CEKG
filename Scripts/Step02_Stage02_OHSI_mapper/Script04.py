import os
from google.cloud import bigquery

symPath = '../gcKey/SNOMED_CT_Google_Cloud.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            
CREATE TABLE  `snomed-ct-ml4219.K02_SCT.CONCEPT_RELATIONSHIP2`   AS
SELECT 
a.concept_id_1,
b.vocabulary_id as vocabulary_id_1,
a.concept_id_2,
a.relationship_id,
a.valid_start_date,
a.valid_end_date,
a.invalid_reason,
FROM `snomed-ct-ml4219.K02_SCT.CONCEPT_RELATIONSHIP` a
left join `snomed-ct-ml4219.K02_SCT.Concept` b
on a.concept_id_1=b.concept_id;
'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
