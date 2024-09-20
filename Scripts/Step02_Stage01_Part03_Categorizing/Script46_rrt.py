import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create schema `your-project-id1234.N10_RRT` ;

create table `your-project-id1234.N10_RRT.first_day_rrt` as
SELECT * FROM `your-project-id1234.S_Derived6.first_day_rrt` ;

create table `your-project-id1234.N10_RRT.rrt` as
SELECT * FROM `your-project-id1234.S_Derived6.rrt` ;




'''






QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
