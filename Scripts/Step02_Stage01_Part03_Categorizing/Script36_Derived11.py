import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
#print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create schema `your-project-id1234.S_Derived11` ;

create table `your-project-id1234.S_Derived11.first_day_urine_output` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.first_day_urine_output` ;

create table `your-project-id1234.S_Derived11.urine_output` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.urine_output` ;

create table `your-project-id1234.S_Derived11.urine_output_rate` as
SELECT * FROM `your-project-id1234.x_mimiciv_derived.urine_output_rate` ;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
