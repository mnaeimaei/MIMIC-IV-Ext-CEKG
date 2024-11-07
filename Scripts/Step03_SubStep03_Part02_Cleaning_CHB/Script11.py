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
set fluidLabelValue=NULL
where  fluidLabelValue="-" ;

update  `your-project-id1234.N04_CHB.e_Lab` 
set fluidLabelValue=NULL
where  fluidLabelValue=" " ;

#######################################################



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
