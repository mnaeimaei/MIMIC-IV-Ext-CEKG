import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM09`  AS
SELECT distinct Disorder_ID as ID, Disorder_Origin as Name, ICD10_Root_title as tempValue
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM08` ;

alter table `your-project-id1234.O_NonEvents_ICD3.icdCM09` 
add column Origin String;

update  `your-project-id1234.O_NonEvents_ICD3.icdCM09` 
set Origin = "Disorder"
where Origin is null;



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
