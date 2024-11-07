import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create table `your-project-id1234.N01_NDC.w2`   as  
SELECT 
NdcSearch as NdcID, NdcSearch as Ndc
FROM `your-project-id1234.N01_NDC.w1` ;

UPDATE `your-project-id1234.N01_NDC.w2` 
SET Ndc = REPLACE(Ndc, 'A', '')
WHERE Ndc is not null;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
