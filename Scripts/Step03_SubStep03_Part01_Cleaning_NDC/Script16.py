import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.N01_NDC.w3`   AS
        SELECT * FROM (
        SELECT distinct
        a.NdcID , a.Ndc, b.Drug , b.Gsn ,  b.prod_strength 
        From `your-project-id1234.N01_NDC.w2`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.t4`   as b
        ON
        a.Ndc=b.Ndc )   
    ; 

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
