import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE `your-project-id1234.I01_Log.TabD02A`  AS
        
        SELECT
        a.subject_id,
         a.hadm_id,
        b.RankN,
          a.maxT,
        FROM `your-project-id1234.I01_Log.TabD01A`  a
        LEFT   JOIN (
        SELECT  
        subject_id,maxT,
        Row_number() over (partition by  subject_id order by maxT asc) as RankN
        FROM
        
        (SELECT   DISTINCT subject_id,maxT  FROM `your-project-id1234.I01_Log.TabD01A`  )  ) b
        ON
        a.subject_id = b.subject_id AND a.maxT = b.maxT;
        
        
        ##################################
        
        
                CREATE TABLE `your-project-id1234.I01_Log.TabD02B`  AS
        
        SELECT
        a.subject_id,
         a.hadm_id,
        b.RankN,
          a.maxT,
        FROM `your-project-id1234.I01_Log.TabD01B`  a
        LEFT   JOIN (
        SELECT  
        subject_id,maxT,
        Row_number() over (partition by  subject_id order by maxT asc) as RankN
        FROM
        
        (SELECT   DISTINCT subject_id,maxT  FROM `your-project-id1234.I01_Log.TabD01B`  )  ) b
        ON
        a.subject_id = b.subject_id AND a.maxT = b.maxT;
        

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
