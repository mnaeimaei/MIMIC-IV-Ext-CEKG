import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''       


create table `your-project-id1234.N01_NDC.v1`   as     
SELECT distinct
 CONCAT('A', Ndc) NdcSearch, Ndc
FROM `your-project-id1234.N01_NDC.u1` 
where ndc is not null



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
