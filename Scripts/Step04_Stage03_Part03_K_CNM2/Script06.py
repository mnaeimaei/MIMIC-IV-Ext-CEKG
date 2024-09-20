import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

update   `your-project-id1234.K03_SCT.B_ICD`  
set SCDID="443781008"
where  icd_code="Z72"  ;

update   `your-project-id1234.K03_SCT.B_ICD`  
set SCDID="274530001"
where  icd_code="R92"  ;


update   `your-project-id1234.K03_SCT.B_ICD`  
set SCDID="88959008"
where  icd_code="W46"  ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
