import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

alter table  `your-project-id1234.N13_weight.first_day_weight_time` 
add column weight_type string;

update `your-project-id1234.N13_weight.first_day_weight_time` 
set weight_type="first day"
where weight_type is null;



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
