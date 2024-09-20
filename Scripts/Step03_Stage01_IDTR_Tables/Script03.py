import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

drop table  `your-project-id1234.R_TimeA.Table006`  ;
drop table  `your-project-id1234.R_TimeA.Table007`  ;
drop table  `your-project-id1234.R_TimeA.Table015`  ;
drop table  `your-project-id1234.R_TimeA.Table016`  ;
drop table  `your-project-id1234.R_TimeA.Table051`  ;
drop table  `your-project-id1234.R_TimeA.Table052`  ;
drop table  `your-project-id1234.R_TimeA.Table053`  ;
drop table  `your-project-id1234.R_TimeA.Table054`  ;
drop table  `your-project-id1234.R_TimeA.Table055`  ;
drop table  `your-project-id1234.R_TimeA.Table056`  ;
drop table  `your-project-id1234.R_TimeA.Table057`  ;
drop table  `your-project-id1234.R_TimeA.Table058`  ;
drop table  `your-project-id1234.R_TimeA.Table059`  ;
drop table  `your-project-id1234.R_TimeA.Table081`  ;

##########################################################################

drop table  `your-project-id1234.R_TimeA.Table016_B`  ;
drop table  `your-project-id1234.R_TimeA.Table051_B`  ;
drop table  `your-project-id1234.R_TimeA.Table052_B`  ;
drop table  `your-project-id1234.R_TimeA.Table053_B`  ;
drop table  `your-project-id1234.R_TimeA.Table054_B`  ;
drop table  `your-project-id1234.R_TimeA.Table055_B`  ;
drop table  `your-project-id1234.R_TimeA.Table056_B`  ;
drop table  `your-project-id1234.R_TimeA.Table057_B`  ;
drop table  `your-project-id1234.R_TimeA.Table058_B`  ;
drop table  `your-project-id1234.R_TimeA.Table059_B`  ;
drop table  `your-project-id1234.R_TimeA.Table081_B`  ;

##########################################################################

drop table  `your-project-id1234.R_TimeA.Table016_C`  ;
drop table  `your-project-id1234.R_TimeA.Table081_C`  ;

##########################################################################

create table  `your-project-id1234.R_TimeA.Table006`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table006_B` ;

create table  `your-project-id1234.R_TimeA.Table007`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table007_B` ;

create table  `your-project-id1234.R_TimeA.Table015`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table015_B` ;

create table  `your-project-id1234.R_TimeA.Table016`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table016_D` ;

create table  `your-project-id1234.R_TimeA.Table051`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table051_C` ;

create table  `your-project-id1234.R_TimeA.Table052`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table052_C` ;

create table  `your-project-id1234.R_TimeA.Table053`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table053_C` ;

create table  `your-project-id1234.R_TimeA.Table054`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table054_C` ;

create table  `your-project-id1234.R_TimeA.Table055`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table055_C` ;

create table  `your-project-id1234.R_TimeA.Table056`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table056_C` ;

create table  `your-project-id1234.R_TimeA.Table057`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table057_C` ;

create table  `your-project-id1234.R_TimeA.Table058`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table058_C` ;

create table  `your-project-id1234.R_TimeA.Table059`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table059_C` ;

create table  `your-project-id1234.R_TimeA.Table081`  as
SELECT * FROM `your-project-id1234.R_TimeA.Table081_D` ;
##########################################################################

drop table  `your-project-id1234.R_TimeA.Table006_B` ;
drop table  `your-project-id1234.R_TimeA.Table007_B` ;
drop table  `your-project-id1234.R_TimeA.Table015_B` ;
drop table  `your-project-id1234.R_TimeA.Table016_D` ;
drop table  `your-project-id1234.R_TimeA.Table051_C` ;
drop table  `your-project-id1234.R_TimeA.Table052_C` ;
drop table  `your-project-id1234.R_TimeA.Table053_C` ;
drop table  `your-project-id1234.R_TimeA.Table054_C` ;
drop table  `your-project-id1234.R_TimeA.Table055_C` ;
drop table  `your-project-id1234.R_TimeA.Table056_C` ;
drop table  `your-project-id1234.R_TimeA.Table057_C` ;
drop table  `your-project-id1234.R_TimeA.Table058_C` ;
drop table  `your-project-id1234.R_TimeA.Table059_C` ;
drop table  `your-project-id1234.R_TimeA.Table081_D` ;

#####################################################################
'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
