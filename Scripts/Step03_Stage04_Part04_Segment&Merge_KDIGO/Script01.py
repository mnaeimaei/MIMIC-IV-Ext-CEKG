import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create schema `your-project-id1234.N05_KDIGO` ;

create table `your-project-id1234.N05_KDIGO.kdigo_creatinine` as
SELECT * FROM `your-project-id1234.S_Derived5.kdigo_creatinine` ;

create table `your-project-id1234.N05_KDIGO.kdigo_stages` as
SELECT * FROM `your-project-id1234.S_Derived5.kdigo_stages` ;


create table `your-project-id1234.N05_KDIGO.kdigo_uo` as
SELECT * FROM `your-project-id1234.S_Derived5.kdigo_uo` ;















'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
