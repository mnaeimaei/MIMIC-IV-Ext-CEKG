import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

update  `your-project-id1234.N04_CHB.e_Lab` 
set hadm_id=0
where  hadm_id is null ;


update  `your-project-id1234.N04_CHB.e_Lab` 
set fluid="urine sample (random) Culture test"
where  fluid="urine sample (random) culture test" ;

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
