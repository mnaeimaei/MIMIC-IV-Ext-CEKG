import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



#################################################################################

      CREATE TABLE `your-project-id1234.O_NonEvents_ClusData.Q02_Admissions`  AS
        
        SELECT
        a.subject_id, a.hadm_id,
        
        a.insurance, a.language, a.marital_status, a.race, b.RankN
        FROM `your-project-id1234.O_NonEvents_ClusData.Q01_Admissions`  a
        LEFT   JOIN (
        SELECT  
        subject_id, hadm_id,
        Row_number() over (partition by  subject_id order by hadm_id asc) as RankN
        FROM
        
        (SELECT   DISTINCT subject_id, hadm_id  FROM `your-project-id1234.O_NonEvents_ClusData.Q01_Admissions`  )  ) b
        ON
        a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id;
        

##########################################################################





'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
