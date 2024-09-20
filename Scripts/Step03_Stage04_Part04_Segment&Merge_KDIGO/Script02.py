import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create table `your-project-id1234.N05_KDIGO.TabA1` as
SELECT * FROM `your-project-id1234.N05_KDIGO.kdigo_creatinine` 
where charttime is not null;

create table `your-project-id1234.N05_KDIGO.TabA2` as
SELECT * FROM `your-project-id1234.N05_KDIGO.kdigo_stages` 
where charttime is not null;

create table `your-project-id1234.N05_KDIGO.TabA3` as
SELECT * FROM `your-project-id1234.N05_KDIGO.kdigo_uo` 
where charttime is not null;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
