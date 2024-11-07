import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            
         CREATE TABLE  `your-project-id1234.N14_urine.urine_output_subject`   AS
        SELECT distinct * FROM (
        SELECT
       b.subject_id , a.stay_id , a.charttime , a.urineoutput,
        
        From `your-project-id1234.N14_urine.urine_output`   as a
        LEFT JOIN `your-project-id1234.R_TimeC.SI`   as b
        ON
        a.stay_id=b.stay_id 


        )    
    ; 

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
