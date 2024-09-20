import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            
    CREATE TABLE  `your-project-id1234.N10_RRT.rrt_final`  as 
    SELECT subject_id , stay_id , charttime , dialysis_present , dialysis_active , dialysis_type 
    FROM `your-project-id1234.N10_RRT.rrt_subject`  

    union distinct


    SELECT subject_id , stay_id , Timestamps , dialysis_present , dialysis_active , dialysis_type 
    FROM `your-project-id1234.N10_RRT.first_day_rrt_time`  


    

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
