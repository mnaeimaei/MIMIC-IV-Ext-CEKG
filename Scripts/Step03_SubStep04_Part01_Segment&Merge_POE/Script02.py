import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create table `your-project-id1234.N02_POE.1_POE_Tube` as
SELECT * FROM `your-project-id1234.N02_POE.POE` 
where field_name="Tubes & Drains type"	;





create table `your-project-id1234.N02_POE.2_POE_Consult` as
SELECT * FROM `your-project-id1234.N02_POE.POE` 
where field_name="Consult Status Time"	or field_name="Consult Status" or field_name="Level of Urgency";





create table `your-project-id1234.N02_POE.4_POE_Transfer` as
SELECT * FROM `your-project-id1234.N02_POE.POE` 
where field_name="Transfer to"	;


create table `your-project-id1234.N02_POE.5_POE_Discharge_W` as
SELECT * FROM `your-project-id1234.N02_POE.POE` 
where field_name="Discharge When"	;


create table `your-project-id1234.N02_POE.6_POE_Discharge_P` as
SELECT * FROM `your-project-id1234.N02_POE.POE` 
where field_name="Discharge Planning"	;


create table `your-project-id1234.N02_POE.7_POE_Indication` as
SELECT * FROM `your-project-id1234.N02_POE.POE` 
where field_name="Indication"	;


create table `your-project-id1234.N02_POE.8_POE_Admit` as
SELECT * FROM `your-project-id1234.N02_POE.POE` 
where field_name="Admit to"	;

create table `your-project-id1234.N02_POE.9_POE_Admit_Cat` as
SELECT * FROM `your-project-id1234.N02_POE.POE` 
where field_name="Admit category"	;


create table `your-project-id1234.N02_POE.10_POE_Urgencey` as
SELECT * FROM `your-project-id1234.N02_POE.POE` 
where field_name="Level of Urgency"	;



create table `your-project-id1234.N02_POE.11_POE_Code` as
SELECT * FROM `your-project-id1234.N02_POE.POE` 
where field_name="Code status"	;






'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
