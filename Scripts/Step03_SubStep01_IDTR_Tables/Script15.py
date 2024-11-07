import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



create schema `your-project-id1234.R_TimeC` ;

#############################################################

create table  `your-project-id1234.R_TimeC.SH` as	SELECT distinct  subject_id,CAST(hadm_id AS INT64) AS hadm_id,min, max	FROM  `your-project-id1234.R_TimeB.SH` ;
create table  `your-project-id1234.R_TimeC.S` as	SELECT distinct  subject_id, min, max	FROM  `your-project-id1234.R_TimeB.S` ;
create table  `your-project-id1234.R_TimeC.H` as	SELECT distinct  CAST(hadm_id AS INT64) AS hadm_id, min, max	FROM  `your-project-id1234.R_TimeB.H` ;
create table  `your-project-id1234.R_TimeC.I` as	SELECT distinct  stay_id, careunit, min, max	FROM  `your-project-id1234.R_TimeB.I` ;
create table  `your-project-id1234.R_TimeC.SI` as	SELECT distinct  subject_id,stay_id,careunit, min, max	FROM  `your-project-id1234.R_TimeB.SI` ;
create table  `your-project-id1234.R_TimeC.ST` as	SELECT distinct  subject_id,transfer_id,eventtype,careunit,  min, max	FROM  `your-project-id1234.R_TimeB.ST` ;
create table  `your-project-id1234.R_TimeC.HI` as	SELECT distinct  CAST(hadm_id AS INT64) AS hadm_id,stay_id,careunit, min, max	FROM  `your-project-id1234.R_TimeB.HI` ;
create table  `your-project-id1234.R_TimeC.SHT` as	SELECT distinct  subject_id,CAST(hadm_id AS INT64) AS hadm_id,transfer_id,eventtype,careunit, min, max	FROM  `your-project-id1234.R_TimeB.SHT` ;
create table  `your-project-id1234.R_TimeC.HT` as	SELECT distinct  CAST(hadm_id AS INT64) AS hadm_id,transfer_id,eventtype,careunit,  min, max	FROM  `your-project-id1234.R_TimeB.HT` ;
create table  `your-project-id1234.R_TimeC.SHI` as	SELECT distinct  subject_id,CAST(hadm_id AS INT64) AS hadm_id,stay_id,careunit, min, max	FROM  `your-project-id1234.R_TimeB.SHI` ;
create table  `your-project-id1234.R_TimeC.T` as	SELECT distinct  transfer_id,eventtype,careunit,  min, max	FROM  `your-project-id1234.R_TimeB.T` ;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
