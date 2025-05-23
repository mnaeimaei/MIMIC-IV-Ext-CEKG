import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
#print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create schema `your-project-id1234.S_Derived13` ;

create table `your-project-id1234.S_Derived13.antibiotic` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.antibiotic` ;

create table `your-project-id1234.S_Derived13.sepsis3` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.sepsis3` ;

create table `your-project-id1234.S_Derived13.sofa` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.sofa` ;

create table `your-project-id1234.S_Derived13.suspicion_of_infection` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.suspicion_of_infection` ;




create table `your-project-id1234.S_Derived13.first_day_sofa` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.first_day_sofa` ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
