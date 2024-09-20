import os
from google.cloud import bigquery

symPath = '../gcKey/SNOMED_CT_Google_Cloud.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''          

CREATE TABLE  `snomed-ct-ml4219.K02_SCT.ICD_SNOMED6`   AS
SELECT distinct * from (
SELECT concept_code_1 as ICD10CM, concept_code_2 as SCDID FROM `snomed-ct-ml4219.K02_SCT.ICD_SNOMED5` 
where vocabulary_id_1="ICD10CM" and vocabulary_id_2="SNOMED"
union all
SELECT concept_code_2 as ICD10CM, concept_code_1 as SCDID FROM `snomed-ct-ml4219.K02_SCT.ICD_SNOMED5` 
where vocabulary_id_1="SNOMED" and vocabulary_id_2="ICD10CM")





'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
