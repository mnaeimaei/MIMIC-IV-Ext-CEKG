import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM15`  AS
        
        SELECT
        a.hadm_id, a.Disorder_ID_agg,
        b.RankN mm_ID
        
        FROM `your-project-id1234.O_NonEvents_ICD3.icdCM14`  a
        LEFT   JOIN (
        SELECT  
        num,Disorder_ID_agg,
        Row_number() over (partition by  num order by Disorder_ID_agg asc) as RankN
        FROM
        
        (SELECT   DISTINCT num,Disorder_ID_agg  FROM `your-project-id1234.O_NonEvents_ICD3.icdCM14`  )  ) b
        ON
        a.num = b.num AND a.Disorder_ID_agg = b.Disorder_ID_agg;
        


        
        
'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
