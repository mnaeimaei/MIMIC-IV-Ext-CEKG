import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create table `your-project-id1234.N03_OMR.Temp`   as
Select * from `your-project-id1234.N03_OMR.OMR01`  ;

ALTER TABLE `your-project-id1234.N03_OMR.Temp`  
ADD column  c1 STRING ;

UPDATE `your-project-id1234.N03_OMR.Temp`  
SET c1 ="A"
WHERE c1 is null ;

create table `your-project-id1234.N03_OMR.OMR02`   as
Select 
Row_number() over (partition by c1 order by subject_id) as NewID, subject_id, chartdate
from `your-project-id1234.N03_OMR.Temp`  ;

drop table `your-project-id1234.N03_OMR.Temp`  ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
