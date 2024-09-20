import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create table `your-project-id1234.R_TimeA.Table006_B` as 
SELECT 
subject_id, hadm_id, DATETIME(Timestamp) AS Timestamp,  Title
FROM `your-project-id1234.R_TimeA.Table006` ;

############################################################################

create table `your-project-id1234.R_TimeA.Table007_B` as 
SELECT 
subject_id, hadm_id, DATETIME(Timestamp) AS Timestamp,  Title
FROM `your-project-id1234.R_TimeA.Table007` ;

############################################################################

create table `your-project-id1234.R_TimeA.Table015_B` as 
SELECT 
subject_id, DATETIME(Timestamp) AS Timestamp,  Title
FROM `your-project-id1234.R_TimeA.Table015` ;

############################################################################

create table `your-project-id1234.R_TimeA.Table016_B` as 
SELECT 
subject_id, DATETIME(Timestamp) AS Timestamp,  Title
FROM `your-project-id1234.R_TimeA.Table016` ;


create table  `your-project-id1234.R_TimeA.Table016_C` as
SELECT 
subject_id, FORMAT_DATETIME("%Y-%m-%dT23:59:59", Timestamp) as Timestamp, Title
FROM `your-project-id1234.R_TimeA.Table016_B`;


create table  `your-project-id1234.R_TimeA.Table016_D` as
SELECT 

subject_id,
DATETIME(Timestamp) AS Timestamp, 
Title
FROM `your-project-id1234.R_TimeA.Table016_C` 


############################################################################
create table `your-project-id1234.R_TimeA.Table081_B` as 
SELECT 
subject_id, DATETIME(Timestamp) AS Timestamp,  Title
FROM `your-project-id1234.R_TimeA.Table081` ;

create table  `your-project-id1234.R_TimeA.Table081_C` as
SELECT 
subject_id, FORMAT_DATETIME("%Y-%m-%dT23:59:59", Timestamp) as Timestamp, Title
FROM `your-project-id1234.R_TimeA.Table081_B`;


create table  `your-project-id1234.R_TimeA.Table081_D` as
SELECT 
,subject_id,
DATETIME(Timestamp) AS Timestamp, 
Title
FROM `your-project-id1234.R_TimeA.Table081_C` ;

############################################################################

create table  `your-project-id1234.R_TimeA.Table051_B` as
SELECT 
subject_id, 
hadm_id, 
FORMAT_DATETIME('%Y-%m-%dT%H:%M:%S', PARSE_DATETIME('%Y-%m-%d %H:%M:%S', Timestamp)) AS Timestamp, 
Title
FROM `your-project-id1234.R_TimeA.Table051` ;


create table  `your-project-id1234.R_TimeA.Table051_C` as
SELECT 
subject_id, 
hadm_id, 
CAST(Timestamp AS DATETIME) as Timestamp, 
Title
FROM `your-project-id1234.R_TimeA.Table051_B` ;

############################################################################

create table  `your-project-id1234.R_TimeA.Table052_B` as
SELECT 
subject_id, 
hadm_id, 
FORMAT_DATETIME('%Y-%m-%dT%H:%M:%S', PARSE_DATETIME('%Y-%m-%d %H:%M:%S', Timestamp)) AS Timestamp, 
Title
FROM `your-project-id1234.R_TimeA.Table052` ;


create table  `your-project-id1234.R_TimeA.Table052_C` as
SELECT 
subject_id, 
hadm_id, 
CAST(Timestamp AS DATETIME) as Timestamp, 
Title
FROM `your-project-id1234.R_TimeA.Table052_B` ;

############################################################################

create table  `your-project-id1234.R_TimeA.Table053_B` as
SELECT 
subject_id, 
hadm_id, 
FORMAT_DATETIME('%Y-%m-%dT%H:%M:%S', PARSE_DATETIME('%Y-%m-%d %H:%M:%S', Timestamp)) AS Timestamp, 
Title
FROM `your-project-id1234.R_TimeA.Table053` ;


create table  `your-project-id1234.R_TimeA.Table053_C` as
SELECT 
subject_id, 
hadm_id, 
CAST(Timestamp AS DATETIME) as Timestamp, 
Title
FROM `your-project-id1234.R_TimeA.Table053_B` ;

############################################################################


create table  `your-project-id1234.R_TimeA.Table054_B` as
SELECT 
subject_id, 
hadm_id, 
FORMAT_DATETIME('%Y-%m-%dT%H:%M:%S', PARSE_DATETIME('%Y-%m-%d %H:%M:%S', Timestamp)) AS Timestamp, 
Title
FROM `your-project-id1234.R_TimeA.Table054` ;


create table  `your-project-id1234.R_TimeA.Table054_C` as
SELECT 
subject_id, 
hadm_id, 
CAST(Timestamp AS DATETIME) as Timestamp, 
Title
FROM `your-project-id1234.R_TimeA.Table054_B` ;

############################################################################################

create table  `your-project-id1234.R_TimeA.Table055_B` as
SELECT subject_id,
hadm_id, stay_id, FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', Timestamp) AS Timestamp, Title
FROM `your-project-id1234.R_TimeA.Table055`;

create table  `your-project-id1234.R_TimeA.Table056_B` as
SELECT subject_id,
hadm_id, stay_id, FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', Timestamp) AS Timestamp, Title
FROM `your-project-id1234.R_TimeA.Table056`;

create table  `your-project-id1234.R_TimeA.Table057_B` as
SELECT subject_id,
stay_id, FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', Timestamp) AS Timestamp, Title
FROM `your-project-id1234.R_TimeA.Table057`;

create table  `your-project-id1234.R_TimeA.Table058_B` as
SELECT subject_id,
stay_id, FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', Timestamp) AS Timestamp, Title
FROM `your-project-id1234.R_TimeA.Table058`;

create table  `your-project-id1234.R_TimeA.Table059_B` as
SELECT subject_id,
stay_id, FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', Timestamp) AS Timestamp, Title
FROM `your-project-id1234.R_TimeA.Table059`;

############################################################################################

create table  `your-project-id1234.R_TimeA.Table055_C` as
SELECT 
subject_id, hadm_id, stay_id, CAST(Timestamp AS DATETIME) as Timestamp, Title
FROM `your-project-id1234.R_TimeA.Table055_B` ;


create table  `your-project-id1234.R_TimeA.Table056_C` as
SELECT 
subject_id, hadm_id, stay_id, CAST(Timestamp AS DATETIME) as Timestamp, Title
FROM `your-project-id1234.R_TimeA.Table056_B` ;


create table  `your-project-id1234.R_TimeA.Table057_C` as
SELECT 
subject_id, stay_id, CAST(Timestamp AS DATETIME) as Timestamp, Title
FROM `your-project-id1234.R_TimeA.Table057_B` ;


create table  `your-project-id1234.R_TimeA.Table058_C` as
SELECT 
subject_id, stay_id, CAST(Timestamp AS DATETIME) as Timestamp,  Title
FROM `your-project-id1234.R_TimeA.Table058_B` ;


create table  `your-project-id1234.R_TimeA.Table059_C` as
SELECT 
subject_id, stay_id, CAST(Timestamp AS DATETIME) as Timestamp, Title
FROM `your-project-id1234.R_TimeA.Table059_B` ;

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
