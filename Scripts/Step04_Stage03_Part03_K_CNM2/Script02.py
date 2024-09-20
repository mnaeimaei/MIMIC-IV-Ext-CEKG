import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



create table `your-project-id1234.K03_SCT.A_ICD` as
SELECT * FROM `your-project-id1234.K01_SCT.A_ICD` ;



create table `your-project-id1234.K03_SCT.IcdSCTmapper` as
SELECT * FROM `your-project-id1234.K02_SCT.ICD_SNOMED6` ;



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
