import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''


       create schema `mimic-four-ml4219.M01_RFE` ;

create table `mimic-four-ml4219.M01_RFE.TabA1` as
SELECT * FROM `mimic-four-ml4219.S_Admission.2_Admissions` ;


create table `mimic-four-ml4219.M01_RFE.TabA2` as
SELECT subject_id, hadm_id, edregtime as Timestamp
FROM `mimic-four-ml4219.M01_RFE.TabA1` 
where edregtime is not null;

alter table `mimic-four-ml4219.M01_RFE.TabA2`
add column Activity string;

alter table `mimic-four-ml4219.M01_RFE.TabA2` 
add column Activity_Synonym string;

update  `mimic-four-ml4219.M01_RFE.TabA2` 
set Activity="Reg_from_ED"
where Activity is null;

update  `mimic-four-ml4219.M01_RFE.TabA2` 
set Activity_Synonym="RFE"
where Activity_Synonym is null;



ALTER TABLE `mimic-four-ml4219.M01_RFE.TabA2`
ADD column  Activity_Value_ID STRING ;

ALTER TABLE `mimic-four-ml4219.M01_RFE.TabA2`  
ADD column  Activity_Properties_ID_aggregation STRING ;




create TABLE `mimic-four-ml4219.M01_RFE.TabA3`  as
select subject_id, hadm_id, Timestamp as Timestamps, Activity, Activity_Synonym, Activity_Value_ID, Activity_Properties_ID_aggregation
FROM `mimic-four-ml4219.M01_RFE.TabA2`  ;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
