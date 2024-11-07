import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

    CREATE TABLE  `your-project-id1234.N13_weight.weight_final`  as 
    
    SELECT subject_id , stay_id , starttime, endtime ,  weight_type , weight 
    FROM `your-project-id1234.N13_weight.first_day_weight_time`  
    
    union distinct
    SELECT subject_id , stay_id , starttime, endtime ,  weight_type , weight 
    FROM `your-project-id1234.N13_weight.weight_durations_subject`  ;
    
    
    

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
