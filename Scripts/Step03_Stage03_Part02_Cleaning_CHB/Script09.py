import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

update  `your-project-id1234.N04_CHB.d_Lab` 
set value=NULL
where  value="___" ;



update  `your-project-id1234.N04_CHB.d_Lab` 
set fluidLabelValue=value
where  value is not null and fluidLabelValue is null;




'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
