import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create schema `your-project-id1234.R_TimeD` ;

#############################################################

create table `your-project-id1234.R_TimeD.SH`  as	SELECT distinct subject_id, hadm_id, min, max, DATE(min) AS minDate, DATE(max) AS maxDate	FROM `your-project-id1234.R_TimeC.SH` ;
create table `your-project-id1234.R_TimeD.S`   as	SELECT distinct subject_id, min, max, DATE(min) AS minDate, DATE(max) AS maxDate	FROM `your-project-id1234.R_TimeC.S` ;
create table `your-project-id1234.R_TimeD.H`   as	SELECT distinct hadm_id, min, max, DATE(min) AS minDate, DATE(max) AS maxDate	FROM `your-project-id1234.R_TimeC.H` ;
create table `your-project-id1234.R_TimeD.I`   as	SELECT distinct stay_id, careunit, min, max, DATE(min) AS minDate, DATE(max) AS maxDate	FROM `your-project-id1234.R_TimeC.I_2` ;
create table `your-project-id1234.R_TimeD.SI`  as	SELECT distinct subject_id, stay_id, careunit, min, max, DATE(min) AS minDate, DATE(max) AS maxDate	FROM `your-project-id1234.R_TimeC.SI_2` ;
create table `your-project-id1234.R_TimeD.ST`  as	SELECT distinct subject_id, transfer_id, eventtype, careunit, min, max, DATE(min) AS minDate, DATE(max) AS maxDate	FROM `your-project-id1234.R_TimeC.ST_2` ;
create table `your-project-id1234.R_TimeD.HI`  as	SELECT distinct hadm_id, stay_id, careunit, min, max, DATE(min) AS minDate, DATE(max) AS maxDate	FROM `your-project-id1234.R_TimeC.HI_2` ;
create table `your-project-id1234.R_TimeD.SHT` as	SELECT distinct subject_id, hadm_id, transfer_id, eventtype, careunit, min, max, DATE(min) AS minDate, DATE(max) AS maxDate	FROM `your-project-id1234.R_TimeC.SHT_2` ;
create table `your-project-id1234.R_TimeD.HT`  as	SELECT distinct hadm_id, transfer_id, eventtype, careunit, min, max, DATE(min) AS minDate, DATE(max) AS maxDate	FROM `your-project-id1234.R_TimeC.HT_2` ;
create table `your-project-id1234.R_TimeD.SHI` as	SELECT distinct subject_id, hadm_id, stay_id, careunit, min, max, DATE(min) AS minDate, DATE(max) AS maxDate	FROM `your-project-id1234.R_TimeC.SHI_2` ;
create table `your-project-id1234.R_TimeD.T`   as	SELECT distinct transfer_id, eventtype, careunit, min, max, DATE(min) AS minDate, DATE(max) AS maxDate	FROM `your-project-id1234.R_TimeC.T_2` ;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
