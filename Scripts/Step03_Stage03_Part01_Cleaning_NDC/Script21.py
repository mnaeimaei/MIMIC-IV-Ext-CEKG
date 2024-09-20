import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            
        CREATE TABLE  `your-project-id1234.N01_NDC.w7`   AS
        SELECT distinct * FROM (
        SELECT
        a.NdcID , a.Ndc , a.Gsn , a.Drug , b.Drug as Drug2 , a.prod_strength, b.prod_strength  as prod_strenth2,

        b.Ndc as Ndc2 ,  
        From `your-project-id1234.N01_NDC.w6`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.t4`   as b
        ON
        a.Gsn=b.Gsn )
        where Ndc<>    Ndc2
        order by Ndc
    ; 


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
