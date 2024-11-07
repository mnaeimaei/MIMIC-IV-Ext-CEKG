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
set SCDID="94392001"
where  icd_code="C77"  ;

update   `your-project-id1234.K03_SCT.B_ICD`  
set SCDID="269473008"
where  icd_code="C78"  ;


update   `your-project-id1234.K03_SCT.B_ICD`  
set SCDID="216312009"
where  icd_code="Y92"  ;


update   `your-project-id1234.K03_SCT.B_ICD`  
set SCDID="102485007"
where  icd_code="Z91"  ;




##################################################



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
