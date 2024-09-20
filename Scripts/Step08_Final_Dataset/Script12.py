import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''  
          
CREATE TABLE `your-project-id1234.G_mimiciv_ext_cekg.L_CNM4_1`  AS
SELECT 
Activity,
Activity_Synonym,
Domain as Activity_Domain
FROM `your-project-id1234.I15_DK61.TabA` ;

        
        

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
