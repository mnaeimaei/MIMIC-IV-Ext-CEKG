import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
#print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create schema `your-project-id1234.S_Derived14` ;

create table `your-project-id1234.S_Derived14.age` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.age` ;

create table `your-project-id1234.S_Derived14.icustay_detail` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.icustay_detail` ;

create table `your-project-id1234.S_Derived14.icustay_times` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.icustay_times` ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
