import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE `your-project-id1234.I03_EntityRel.TabC`  AS
SELECT  distinct
Entity_Origin1, Entity_ID1, Entity_Origin2, Entity_ID2
FROM `your-project-id1234.I03_EntityRel.TabB` ;
        
        
        

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
