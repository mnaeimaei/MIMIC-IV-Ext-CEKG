import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
#print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create schema `your-project-id1234.S_Derived1` ;

create table `your-project-id1234.S_Derived1.blood_differential` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.blood_differential` ;

create table `your-project-id1234.S_Derived1.cardiac_marker` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.cardiac_marker` ;

create table `your-project-id1234.S_Derived1.chemistry` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.chemistry` ;

create table `your-project-id1234.S_Derived1.coagulation` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.coagulation` ;

create table `your-project-id1234.S_Derived1.complete_blood_count` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.complete_blood_count` ;

create table `your-project-id1234.S_Derived1.crrt` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.crrt` ;

create table `your-project-id1234.S_Derived1.differential_detailed` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.differential_detailed` ;

create table `your-project-id1234.S_Derived1.enzyme` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.enzyme` ;

create table `your-project-id1234.S_Derived1.icp` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.icp` ;

create table `your-project-id1234.S_Derived1.inflammation` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.inflammation` ;

create table `your-project-id1234.S_Derived1.invasive_line` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.invasive_line` ;

create table `your-project-id1234.S_Derived1.oxygen_delivery` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.oxygen_delivery` ;

create table `your-project-id1234.S_Derived1.rhythm` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.rhythm` ;

create table `your-project-id1234.S_Derived1.sapsii` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.sapsii` ;


create table `your-project-id1234.S_Derived1.neuroblock` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.neuroblock` ;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
