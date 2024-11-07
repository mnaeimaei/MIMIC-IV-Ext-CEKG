import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

    CREATE TABLE  `your-project-id1234.N14_urine.urineoutput_output`  as 
    
    
    SELECT subject_id , stay_id , Timestamps , urineoutput 
    FROM `your-project-id1234.N14_urine.first_day_urine_output_time`  
    
    union distinct
    
    SELECT subject_id , stay_id , charttime , urineoutput 
    FROM `your-project-id1234.N14_urine.urine_output_subject`  ;
  
'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
