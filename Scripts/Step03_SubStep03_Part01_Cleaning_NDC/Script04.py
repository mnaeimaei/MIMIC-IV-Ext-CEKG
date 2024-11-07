import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        
    CREATE TABLE  `your-project-id1234.N01_NDC.s1`  as 
    SELECT distinct * from    (
    SELECT subject_id , hadm_id , pharmacy_id , poe_id FROM `your-project-id1234.N01_NDC.p1` 
    union all
    SELECT subject_id , hadm_id , pharmacy_id , poe_id FROM `your-project-id1234.N01_NDC.e1` 
    union all
    SELECT subject_id , hadm_id , pharmacy_id , poe_id FROM `your-project-id1234.N01_NDC.m1` );

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
