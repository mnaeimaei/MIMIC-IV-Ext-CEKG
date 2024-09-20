import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create schema `your-project-id1234.N06_VASO` ;

create table `your-project-id1234.N06_VASO.Tab_agent` as
SELECT * FROM `your-project-id1234.S_Derived10.vasoactive_agent` ;

create table `your-project-id1234.N06_VASO.dobutamine` as
SELECT * FROM `your-project-id1234.S_Derived10.dobutamine` ;

create table `your-project-id1234.N06_VASO.dopamine` as
SELECT * FROM `your-project-id1234.S_Derived10.dopamine` ;

create table `your-project-id1234.N06_VASO.epinephrine` as
SELECT * FROM `your-project-id1234.S_Derived10.epinephrine` ;

create table `your-project-id1234.N06_VASO.milrinone` as
SELECT * FROM `your-project-id1234.S_Derived10.milrinone` ;

create table `your-project-id1234.N06_VASO.norepinephrine` as
SELECT * FROM `your-project-id1234.S_Derived10.norepinephrine` ;

create table `your-project-id1234.N06_VASO.norepinephrine_equivalent_dose` as
SELECT * FROM `your-project-id1234.S_Derived10.norepinephrine_equivalent_dose` ;

create table `your-project-id1234.N06_VASO.phenylephrine` as
SELECT * FROM `your-project-id1234.S_Derived10.phenylephrine` ;

create table `your-project-id1234.N06_VASO.vasopressin` as
SELECT * FROM `your-project-id1234.S_Derived10.vasopressin` ;

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)