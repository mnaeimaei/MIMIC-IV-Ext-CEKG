import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create schema `your-project-id1234.N16_sofa` ;

create table `your-project-id1234.N16_sofa.antibiotic` as
SELECT * FROM `your-project-id1234.S_Derived13.antibiotic` ;

create table `your-project-id1234.N16_sofa.first_day_sofa` as
SELECT * FROM `your-project-id1234.S_Derived13.first_day_sofa` ;

create table `your-project-id1234.N16_sofa.sepsis3` as
SELECT * FROM `your-project-id1234.S_Derived13.sepsis3` ;

create table `your-project-id1234.N16_sofa.sofa` as
SELECT * FROM `your-project-id1234.S_Derived13.sofa` ;

create table `your-project-id1234.N16_sofa.suspicion_of_infection` as
SELECT * FROM `your-project-id1234.S_Derived13.suspicion_of_infection` ;



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
