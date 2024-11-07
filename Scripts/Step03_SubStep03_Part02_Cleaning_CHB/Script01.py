import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


#############################################################

ALTER TABLE  `your-project-id1234.N04_CHB.A1`
ADD column  category STRING ;

ALTER TABLE  `your-project-id1234.N04_CHB.A2`
ADD column  category STRING ;

ALTER TABLE  `your-project-id1234.N04_CHB.A3`
ADD column  category STRING ;

#############################################################

UPDATE   `your-project-id1234.N04_CHB.A1`
SET category ="Chemistry"
WHERE category is null ;

UPDATE  `your-project-id1234.N04_CHB.A2`
SET category ="Hematology"
WHERE category is null ;

UPDATE  `your-project-id1234.N04_CHB.A3`
SET category ="Blood_GAS"
WHERE category is null ;



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
