import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.N01_NDC.w92`   AS
        SELECT distinct NdcID , Ndc , Gsn , Drug , prod_strength  FROM (
        SELECT
        a.NdcID , a.Ndc , a.Gsn , a.Drug , a.prod_strength,
        b.NdcID as temp
        From `your-project-id1234.N01_NDC.w6`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.w91`   as b
        ON
        a.Ndc=b.Ndc )    
        where temp is null

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
