import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

    CREATE TABLE  `your-project-id1234.N12_Height.Height_Final`  as 
    SELECT subject_id , stay_id , charttime , height 
    FROM `your-project-id1234.N12_Height.height`  
    

    union distinct
        SELECT subject_id , stay_id , Timestamps , height 
    FROM `your-project-id1234.N12_Height.first_day_height_time`  



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
