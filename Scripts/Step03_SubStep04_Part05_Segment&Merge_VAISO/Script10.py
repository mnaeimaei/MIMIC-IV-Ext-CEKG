import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE  `your-project-id1234.N06_VASO.TabA51`   AS
SELECT * FROM `your-project-id1234.N06_VASO.TabA4` 
where  linkorderid is not null;

CREATE TABLE  `your-project-id1234.N06_VASO.TabA52`   AS
SELECT * FROM `your-project-id1234.N06_VASO.TabA4` 
where  linkorderid is null;



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
