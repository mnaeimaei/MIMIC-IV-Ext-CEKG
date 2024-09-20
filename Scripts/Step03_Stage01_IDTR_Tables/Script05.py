import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create table `your-project-id1234.R_TimeA.V_SH_Time`  as  
select distinct * from (select * from  `your-project-id1234.R_TimeA.U_SH_Time` )   	
where hadm_id is not null and subject_id is not null	;	


create table `your-project-id1234.R_TimeA.V_S_Time`  as  	
select distinct * from (select * from  `your-project-id1234.R_TimeA.U_S_Time`)   	
where subject_id is not null	;	

create table `your-project-id1234.R_TimeA.V_H_Time`  as  	
select distinct * from (select * from `your-project-id1234.R_TimeA.U_H_Time`)   	
where hadm_id is not null	;	

create table `your-project-id1234.R_TimeA.V_T_Time`  as  	
select distinct * from (select * from  `your-project-id1234.R_TimeA.U_T_Time` )   	
where transfer_id is not null	;	

create table `your-project-id1234.R_TimeA.V_I_Time`  as  	
select distinct * from (select * from  `your-project-id1234.R_TimeA.U_I_Time` )   	
where stay_id is not null	;	

create table `your-project-id1234.R_TimeA.V_SI_Time`  as  	
select distinct * from (select * from  `your-project-id1234.R_TimeA.U_SI_Time` )   	
where stay_id is not null  and subject_id is not null	;	

create table `your-project-id1234.R_TimeA.V_ST_Time`  as  	
select distinct * from (select * from  `your-project-id1234.R_TimeA.U_ST_Time` )   	
where transfer_id is not null and subject_id is not null	;	

create table `your-project-id1234.R_TimeA.V_HT_Time`  as  	
select distinct * from (select * from  `your-project-id1234.R_TimeA.U_HT_Time` )   	
where hadm_id is not null and transfer_id is not null	;	

create table `your-project-id1234.R_TimeA.V_HI_Time`  as  	
select distinct * from (select * from `your-project-id1234.R_TimeA.U_HI_Time` )      
where stay_id is not null and hadm_id is not null       ;    
    
create table `your-project-id1234.R_TimeA.V_SHI_Time`  as  	
select distinct * from (select * from  `your-project-id1234.R_TimeA.U_SHI_Time` )   	
where subject_id is not null and hadm_id is not null and stay_id is not null 	;	

create table `your-project-id1234.R_TimeA.V_SHT_Time`  as  	
select distinct * from (select * from `your-project-id1234.R_TimeA.U_SHT_Time`)   	
where subject_id is not null and hadm_id is not null and transfer_id is not null 	;	

create table `your-project-id1234.R_TimeA.V_S_Date`  as  	
select distinct * from (select * from  `your-project-id1234.R_TimeA.U_S_Date`  )   	
where subject_id is not null	;	

create table `your-project-id1234.R_TimeA.V_SH_Date`  as  	
select distinct * from (select * from  `your-project-id1234.R_TimeA.U_SH_Date`)   	
where subject_id is not null and hadm_id is not null	;	

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
