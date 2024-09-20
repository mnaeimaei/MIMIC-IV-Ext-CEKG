import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)



os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.N01_NDC.w91`   AS
        SELECT distinct NdcID , Ndc , Gsn2ID , Gsn , Drug , Drug2 ,prod_strength , prod_strenth2 , Ndc2ID , Ndc2, FROM (
        SELECT
        a.NdcID , a.Ndc , a.Gsn2ID , a.Gsn , a.Drug , a.Drug2 , a.prod_strength , a.prod_strenth2 , a.Ndc2ID , a.Ndc2,
         b.Ndc  as Ndc1
        From `your-project-id1234.N01_NDC.w8`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.w7`   as b
        ON
        a.Ndc2=b.Ndc )   
        where Ndc1 is null 

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
