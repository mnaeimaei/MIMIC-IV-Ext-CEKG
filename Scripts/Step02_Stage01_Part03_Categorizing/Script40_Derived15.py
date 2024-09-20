import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
#print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create schema `your-project-id1234.S_Derived15` ;


create table `your-project-id1234.S_Derived15.apsiii` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.apsiii` ;

create table `your-project-id1234.S_Derived15.charlson` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.charlson` ;

create table `your-project-id1234.S_Derived15.creatinine_baseline` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.creatinine_baseline` ;

create table `your-project-id1234.S_Derived15.icustay_hourly` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.icustay_hourly` ;

create table `your-project-id1234.S_Derived15.lods` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.lods` ;

create table `your-project-id1234.S_Derived15.meld` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.meld` ;

create table `your-project-id1234.S_Derived15.oasis` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.oasis` ;

create table `your-project-id1234.S_Derived15.sirs` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.sirs` ;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
