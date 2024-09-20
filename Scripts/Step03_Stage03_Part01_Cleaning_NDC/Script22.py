import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            
        CREATE TABLE  `your-project-id1234.N01_NDC.w8`   AS
        SELECT distinct * FROM (
        SELECT
        NdcID , Ndc , concat("A",Gsn) as Gsn2ID, Gsn , Drug , Drug2 , prod_strength, prod_strenth2, concat("A",Ndc2) as Ndc2ID,
        Ndc2 ,  
        From `your-project-id1234.N01_NDC.w7`   )


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
