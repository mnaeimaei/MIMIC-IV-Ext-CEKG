import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM17_other_ent`  AS
SELECT distinct 
Origin,
ID,
Name,
Value,
Category
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM16` 

union distinct

SELECT distinct 
Origin,
ID,
Name,
Value,
Category
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM11` ;

##########################################################################################


CREATE TABLE  `your-project-id1234.O_NonEvents_ICD3.icdCM17_Temp1`   AS
SELECT * FROM (
SELECT
a.Origin , a.ID , a.Name , a.Value , a.Category,
b.hadm_id,
From `your-project-id1234.O_NonEvents_ICD3.icdCM17_other_ent`   as a
LEFT JOIN `your-project-id1234.O_NonEvents_ICD3.icdCM12_int`   as b
ON
a.ID=b.Disorder_ID )    ; 

update `your-project-id1234.O_NonEvents_ICD3.icdCM17_Temp1` 
set hadm_id=null
where Origin<>"Disorder" ;



CREATE TABLE  `your-project-id1234.O_NonEvents_ICD3.icdCM17_Temp2`   AS
SELECT * FROM (
SELECT
a.Origin , a.ID , a.Name , a.Value , a.Category , a.hadm_id,
b.hadm_id as hadm_id2 ,
From `your-project-id1234.O_NonEvents_ICD3.icdCM17_Temp1`   as a
LEFT JOIN `your-project-id1234.O_NonEvents_ICD3.icdCM15`   as b
ON
a.ID=b.mm_ID )    ; 


update `your-project-id1234.O_NonEvents_ICD3.icdCM17_Temp2` 
set hadm_id2=null
where Origin="Disorder" ;


update `your-project-id1234.O_NonEvents_ICD3.icdCM17_Temp2` 
set hadm_id=hadm_id2
where Origin<>"Disorder" ;


CREATE TABLE  `your-project-id1234.O_NonEvents_ICD3.icdCM17_Temp3`   AS
SELECT distinct
Origin, ID, Name, Value, Category, hadm_id
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM17_Temp2` ;


CREATE TABLE  `your-project-id1234.O_NonEvents_ICD3.icdCM17_other_ent_final`   AS
SELECT * FROM (
SELECT
a.Origin , a.ID , a.Name , a.Value , a.Category , b.subject_id , a.hadm_id
From `your-project-id1234.O_NonEvents_ICD3.icdCM17_Temp3`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SH`   as b
ON
a.hadm_id=b.hadm_id )    ; 

drop TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM17_Temp1` ;
drop TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM17_Temp2` ;
drop TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM17_Temp3` ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
