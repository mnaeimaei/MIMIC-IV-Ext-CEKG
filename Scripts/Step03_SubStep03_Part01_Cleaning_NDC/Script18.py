import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE `your-project-id1234.N01_NDC.w4`  AS
        
        SELECT
        a.NdcID, a.Ndc, a.Gsn, a.Drug,
        b.RankN,
        a.prod_strength
        FROM `your-project-id1234.N01_NDC.w3`  a
        LEFT   JOIN (
        SELECT  
        NdcID, Ndc, Gsn, Drug,
        Row_number() over (partition by  NdcID order by Drug asc) as RankN
        FROM
        
        (SELECT   DISTINCT NdcID, Ndc, Gsn, Drug  FROM `your-project-id1234.N01_NDC.w3`  )  ) b
        ON
        a.NdcID = b.NdcID AND a.Ndc = b.Ndc AND a.Gsn = b.Gsn AND a.Drug = b.Drug;
        
       
'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
