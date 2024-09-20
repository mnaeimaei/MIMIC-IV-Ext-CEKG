import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM16`  AS
SELECT distinct 
mm_ID as ID, 
concat("m",mm_ID) as Name, 
concat("Multimorbidity") as Origin, 
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM15` 

union distinct

SELECT distinct 
mm_ID as ID, 
concat("n",mm_ID) as Name,  
concat("newMorbids") as Origin,  
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM15` 

union distinct

SELECT distinct 
mm_ID as ID, 
concat("t",mm_ID) as Name,  
concat("treatedMorbids") as Origin,  
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM15` 

union distinct

SELECT distinct 
mm_ID as ID, 
concat("u",mm_ID) as Name,  
concat("untreatedMorbids") as Origin,  
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM15` ;



alter table `your-project-id1234.O_NonEvents_ICD3.icdCM16` 
add column Value string;

update  `your-project-id1234.O_NonEvents_ICD3.icdCM16` 
set Value = "0"
where Value is null;



alter table `your-project-id1234.O_NonEvents_ICD3.icdCM16` 
add column Category string;

update  `your-project-id1234.O_NonEvents_ICD3.icdCM16` 
set Category = "Absolute"
where Category is null;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
