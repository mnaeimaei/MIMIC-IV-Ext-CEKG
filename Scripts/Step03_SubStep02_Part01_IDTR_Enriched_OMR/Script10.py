import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE `your-project-id1234.N03_OMR.OMR091`  AS
select * from  `your-project-id1234.N03_OMR.OMR08`  
where min is  null ;


CREATE TABLE `your-project-id1234.N03_OMR.OMR092`  AS
select * from  `your-project-id1234.N03_OMR.OMR08`  
where min is not  null ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
