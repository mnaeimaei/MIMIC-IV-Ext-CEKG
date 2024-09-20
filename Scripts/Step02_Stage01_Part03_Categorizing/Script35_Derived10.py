import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
#print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create schema `your-project-id1234.S_Derived10` ;

create table `your-project-id1234.S_Derived10.vasoactive_agent` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.vasoactive_agent` ;


create table `your-project-id1234.S_Derived10.dobutamine` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.dobutamine` ;

create table `your-project-id1234.S_Derived10.dopamine` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.dopamine` ;

create table `your-project-id1234.S_Derived10.epinephrine` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.epinephrine` ;

create table `your-project-id1234.S_Derived10.milrinone` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.milrinone` ;

create table `your-project-id1234.S_Derived10.norepinephrine` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.norepinephrine` ;

create table `your-project-id1234.S_Derived10.norepinephrine_equivalent_dose` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.norepinephrine_equivalent_dose` ;

create table `your-project-id1234.S_Derived10.phenylephrine` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.phenylephrine` ;

create table `your-project-id1234.S_Derived10.vasopressin` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.vasopressin` ;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
