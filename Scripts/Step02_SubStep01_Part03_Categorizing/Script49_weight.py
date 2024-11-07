import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create schema `your-project-id1234.N13_weight` ;

create table `your-project-id1234.N13_weight.first_day_weight` as
SELECT * FROM `your-project-id1234.S_Derived9.first_day_weight` ;

create table `your-project-id1234.N13_weight.weight_durations` as
SELECT * FROM `your-project-id1234.S_Derived9.weight_durations` ;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
