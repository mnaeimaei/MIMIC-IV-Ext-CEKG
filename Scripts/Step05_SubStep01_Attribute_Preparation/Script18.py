import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE  `your-project-id1234.O_NonEvents_ICD3.icdCM19_adm_mm_final`   AS
SELECT * FROM (
SELECT
a.Entity_Origin1 , a.Entity_ID1 , a.Entity_Origin2 , a.Entity_ID2,
b.subject_id , b.hadm_id 
From `your-project-id1234.O_NonEvents_ICD3.icdCM18_adm_mm`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SH`   as b
ON a.Entity_ID1=b.hadm_id )    ; 

######################################################################################



CREATE TABLE  `your-project-id1234.O_NonEvents_ICD3.icdCM19_mm_new_final`   AS
SELECT * FROM (
SELECT
a.Entity_Origin1 , a.Entity_ID1 , a.Entity_Origin2 , a.Entity_ID2,
b.subject_id , b.hadm_id,
From `your-project-id1234.O_NonEvents_ICD3.icdCM18_mm_new`   as a
LEFT JOIN `your-project-id1234.O_NonEvents_ICD3.icdCM19_adm_mm_final`   as b
ON         a.Entity_ID1=b.Entity_ID2 )        ; 

######################################################################################

CREATE TABLE  `your-project-id1234.O_NonEvents_ICD3.icdCM18_mm_treat_final`   AS
SELECT * FROM (
SELECT
a.Entity_Origin1 , a.Entity_ID1 , a.Entity_Origin2 , a.Entity_ID2,
b.subject_id , b.hadm_id,
From `your-project-id1234.O_NonEvents_ICD3.icdCM18_mm_treat`   as a
LEFT JOIN `your-project-id1234.O_NonEvents_ICD3.icdCM19_adm_mm_final`   as b
ON         a.Entity_ID1=b.Entity_ID2 )        ; 

######################################################################################

CREATE TABLE  `your-project-id1234.O_NonEvents_ICD3.icdCM18_mm_unt_final`   AS
SELECT * FROM (
SELECT
a.Entity_Origin1 , a.Entity_ID1 , a.Entity_Origin2 , a.Entity_ID2,
b.subject_id , b.hadm_id,
From `your-project-id1234.O_NonEvents_ICD3.icdCM18_mm_unt`   as a
LEFT JOIN `your-project-id1234.O_NonEvents_ICD3.icdCM19_adm_mm_final`   as b
ON         a.Entity_ID1=b.Entity_ID2 )        ; 






######################################################################################

CREATE TABLE  `your-project-id1234.O_NonEvents_ICD3.icdCM19_adm_Dis_final`   AS
SELECT * FROM (
SELECT
a.Entity_Origin1 , a.Entity_ID1 , a.Entity_Origin2 , a.Entity_ID2,
b.subject_id , b.hadm_id 
From `your-project-id1234.O_NonEvents_ICD3.icdCM18_adm_Dis`   as a
LEFT JOIN `your-project-id1234.R_TimeD.SH`   as b
ON a.Entity_ID1=b.hadm_id )    ; 





'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
