import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.G_ICD`  AS
SELECT 
ClinicalEntity	as ICD_Origin,
icd_code	as ICD_Code,
icd_version	 as ICD_Version,
icd_code_title as ICD_Code_Title
FROM `your-project-id1234.I06_ICD.Tab_ICD` ;

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
