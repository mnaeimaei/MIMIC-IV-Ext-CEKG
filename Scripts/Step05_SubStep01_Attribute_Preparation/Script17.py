import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM18_adm_mm`  AS
SELECT distinct
concat("Admission") Entity_Origin1,
hadm_id Entity_ID1, 
concat("Multimorbidity") Entity_Origin2,
mm_ID Entity_ID2,
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM15` ;



CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM18_mm_new`  AS
SELECT distinct
concat("Multimorbidity") Entity_Origin1,
mm_ID Entity_ID1, 
concat("newMorbids") Entity_Origin2,
mm_ID Entity_ID2,
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM15` ;



CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM18_mm_treat`  AS
SELECT distinct
concat("Multimorbidity") Entity_Origin1,
mm_ID Entity_ID1, 
concat("treatedMorbids") Entity_Origin2,
mm_ID Entity_ID2,
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM15` ;



CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM18_mm_unt`  AS
SELECT distinct
concat("Multimorbidity") Entity_Origin1,
mm_ID Entity_ID1, 
concat("untreatedMorbids") Entity_Origin2,
mm_ID Entity_ID2,
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM15` ;

CREATE TABLE `your-project-id1234.O_NonEvents_ICD3.icdCM18_adm_Dis`  AS
SELECT distinct
concat("Admission") Entity_Origin1,
hadm_id Entity_ID1, 
concat("Disorder") Entity_Origin2,
Disorder_ID_agg Entity_ID2,
FROM `your-project-id1234.O_NonEvents_ICD3.icdCM15` ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
