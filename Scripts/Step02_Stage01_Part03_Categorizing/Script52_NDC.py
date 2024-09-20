import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



create schema `your-project-id1234.N01_NDC` ;


create table `your-project-id1234.N01_NDC.p1` as
SELECT * FROM `your-project-id1234.S_pharmacy.1_pharmacy` ;

create table `your-project-id1234.N01_NDC.e1` as
SELECT * FROM `your-project-id1234.S_eMAR.1_eMAR` ;

create table `your-project-id1234.N01_NDC.m1` as
SELECT * FROM `your-project-id1234.S_prescriptions.1_prescriptions` ;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
