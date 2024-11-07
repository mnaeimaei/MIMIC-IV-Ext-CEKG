import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            





create table    `your-project-id1234.O_NonEvents_ICD6.icdCM444` as
select distinct
subject_id,
hadm_id
from  `your-project-id1234.O_NonEvents_ICD6.icdCM3`  ;



CREATE TABLE `your-project-id1234.O_NonEvents_ICD6.icdCM445`  AS
SELECT
a.subject_id, a.hadm_id,
b.Dup
FROM `your-project-id1234.O_NonEvents_ICD6.icdCM444`  a
LEFT   JOIN (
SELECT  
subject_id, COUNT(*) dup
FROM `your-project-id1234.O_NonEvents_ICD6.icdCM444` 
GROUP BY subject_id) b
on 
a.subject_id = b.subject_id;
       

drop table  `your-project-id1234.O_NonEvents_ICD6.icdCM444` ;


create table    `your-project-id1234.O_NonEvents_ICD6.icdCM333` as
select distinct
hadm_id,
ICD10
from  `your-project-id1234.O_NonEvents_ICD6.icdCM3`  ;


CREATE TABLE `your-project-id1234.O_NonEvents_ICD6.icdCM335`  AS
SELECT
a.hadm_id, a.ICD10,
b.Dup
FROM `your-project-id1234.O_NonEvents_ICD6.icdCM333`  a
LEFT   JOIN (
SELECT  
hadm_id, COUNT(*) dup
FROM `your-project-id1234.O_NonEvents_ICD6.icdCM333` 
GROUP BY hadm_id) b
on 
a.hadm_id = b.hadm_id;


drop table  `your-project-id1234.O_NonEvents_ICD6.icdCM333` ;


ALTER TABLE  `your-project-id1234.O_NonEvents_ICD6.icdCM335` 
DROP COLUMN ICD10;


ALTER TABLE  `your-project-id1234.O_NonEvents_ICD6.icdCM445` 
DROP COLUMN hadm_id;


CREATE TABLE `your-project-id1234.O_NonEvents_ICD6.T1`  AS
select distinct * from 
 `your-project-id1234.O_NonEvents_ICD6.icdCM335` ;

CREATE TABLE `your-project-id1234.O_NonEvents_ICD6.T2`  AS
select distinct * from 
  `your-project-id1234.O_NonEvents_ICD6.icdCM445` ;


drop table  `your-project-id1234.O_NonEvents_ICD6.icdCM335` ;
drop table  `your-project-id1234.O_NonEvents_ICD6.icdCM445` ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
