import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create schema `your-project-id1234.N11_Vital` ;

create table `your-project-id1234.N11_Vital.first_day_vitalsign` as
SELECT * FROM `your-project-id1234.S_Derived7.first_day_vitalsign` ;

create table `your-project-id1234.N11_Vital.vitalsign` as
SELECT * FROM `your-project-id1234.S_Derived7.vitalsign` ;



'''






QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
