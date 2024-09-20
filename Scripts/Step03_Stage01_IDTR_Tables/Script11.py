import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create schema `your-project-id1234.R_TimeB` ;

#############################################################

create table  `your-project-id1234.R_TimeB.SH` as	SELECT distinct  subject_id,hadm_id,min, max	FROM  `your-project-id1234.R_TimeA.Y_SH` ;
create table  `your-project-id1234.R_TimeB.S` as	SELECT distinct  subject_id, min, max	FROM  `your-project-id1234.R_TimeA.Y_S` ;
create table  `your-project-id1234.R_TimeB.H` as	SELECT distinct  hadm_id, mini as min, maxi as max	FROM  `your-project-id1234.R_TimeA.Y_H` ;
create table  `your-project-id1234.R_TimeB.I` as	SELECT distinct  stay_id, mini as min, maxi as max	FROM  `your-project-id1234.R_TimeA.Y_I` ;
create table  `your-project-id1234.R_TimeB.SI` as	SELECT distinct  subject_id,stay_id, mini as min, maxi as max	FROM  `your-project-id1234.R_TimeA.Y_SI` ;
create table  `your-project-id1234.R_TimeB.ST` as	SELECT distinct  subject_id,transfer_id, mini as min, maxi as max	FROM  `your-project-id1234.R_TimeA.Y_ST` ;
create table  `your-project-id1234.R_TimeB.HI` as	SELECT distinct  hadm_id,stay_id, mini as min, maxi as max	FROM  `your-project-id1234.R_TimeA.Y_HI` ;
create table  `your-project-id1234.R_TimeB.SHT` as	SELECT distinct  subject_id,hadm_id,transfer_id, mini as min, maxi as max	FROM  `your-project-id1234.R_TimeA.Y_SHT` ;
create table  `your-project-id1234.R_TimeB.HT` as	SELECT distinct  hadm_id,transfer_id, mini as min, maxi as max	FROM  `your-project-id1234.R_TimeA.Y_HT` ;
create table  `your-project-id1234.R_TimeB.SHI` as	SELECT distinct  subject_id,hadm_id,stay_id, mini as min, maxi as max	FROM  `your-project-id1234.R_TimeA.Y_SHI` ;
create table  `your-project-id1234.R_TimeB.T` as	SELECT distinct  transfer_id, mini as min, maxi as max	FROM  `your-project-id1234.R_TimeA.Y_T` ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
