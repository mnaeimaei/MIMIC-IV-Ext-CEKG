import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        CREATE TABLE  `your-project-id1234.O_NonEvents_ICD3.icdCM11`   AS
        SELECT * FROM (
        SELECT
        a.Origin, a.ID , a.Name , b.Final as Value,
        From `your-project-id1234.O_NonEvents_ICD3.icdCM9`   as a
        LEFT JOIN `your-project-id1234.O_NonEvents_ICD3.icdCM10`   as b
        ON
        a.Name=b.Name )    
    ; 
    
    
    alter table   `your-project-id1234.O_NonEvents_ICD3.icdCM11`   
    add column Category string;
    
    update   `your-project-id1234.O_NonEvents_ICD3.icdCM11`   
    set Category="Absolute"
    where Category is  null;






'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
