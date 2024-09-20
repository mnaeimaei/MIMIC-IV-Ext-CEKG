import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create schema `your-project-id1234.N04_CHB` ;

create table `your-project-id1234.N04_CHB.A1` as
SELECT * FROM `your-project-id1234.S_labEvents.2_Chemistry` ;

create table `your-project-id1234.N04_CHB.A2` as
SELECT * FROM `your-project-id1234.S_labEvents.2_Hematology` ;


create table `your-project-id1234.N04_CHB.A3` as
SELECT * FROM `your-project-id1234.S_labEvents.2_Blood_Gas` ;




'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
