import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM08`  AS

SELECT distinct

a.subject_id, a.hadm_id,a.seq_num, a.long_title, a.ICD10,a.ICD10_Root, a.ICD10_Root_title, a.MC_num, a.hadm_num, b.RankN as Disorder_ID,
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM07`  a
LEFT   JOIN (
SELECT  
num,ICD10_Root,
Row_number() over (partition by  num order by ICD10_Root asc) as RankN
FROM

(SELECT   DISTINCT num,ICD10_Root  FROM `your-project-id1234.O_NonEvents_ICD3.icdCM07`  )  ) b
ON
a.num = b.num AND a.ICD10_Root = b.ICD10_Root;
        


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
