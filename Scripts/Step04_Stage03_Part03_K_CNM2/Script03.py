import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.K03_SCT.B_ICD`   AS
        SELECT distinct * FROM (
        SELECT
        a.ClinicalEntity , a.icd_code , a.icd_version , a.icd_code_title, b.SCDID 
        From `your-project-id1234.K03_SCT.A_ICD`   as a
        LEFT JOIN `your-project-id1234.K03_SCT.IcdSCTmapper`   as b
        ON
        a.icd_code=b.ICD10CM )    
    ; 


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
