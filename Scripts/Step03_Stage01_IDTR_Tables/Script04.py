import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


###########################################################################		
create table `your-project-id1234.R_TimeA.U_SH_Time`  as  
select distinct * from (
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table001`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table002`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table003`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table004`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table005`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table008`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table009`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table010`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table012`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table014`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table017`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table018`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table019`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table020`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table021`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table022`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table023`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table024`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table025`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table026`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table027`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table028`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table031`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table032`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table052`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table053`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table054`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table060`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table063`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table064`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table065`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table066`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table067`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table068`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table070`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table075`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table089`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table033`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table034`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table035`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table036`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table037`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table038`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table039`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table040`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table041`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table042`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table043`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table044`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table045`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table046`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table047`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table048`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table049`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table050`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table055`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table056`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table061`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table062`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table082`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table083`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table084`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table085`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table087`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table088`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table093`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table108`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table109`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table116`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table117`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table118`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table029`	union all   
SELECT  subject_id, hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table030`	
)   	where Timestamp is not null	;
###########################################################################	
create table `your-project-id1234.R_TimeA.U_S_Time`  as  	
select distinct * from (
SELECT  subject_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table106`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table001`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table002`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table003`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table004`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table005`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table008`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table009`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table010`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table012`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table014`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table017`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table018`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table019`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table020`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table021`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table022`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table023`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table024`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table025`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table026`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table027`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table028`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table031`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table032`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table052`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table053`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table054`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table060`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table063`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table064`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table065`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table066`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table067`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table068`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table070`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table075`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table089`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table033`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table034`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table035`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table036`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table037`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table038`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table039`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table040`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table041`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table042`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table043`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table044`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table045`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table046`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table047`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table048`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table049`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table050`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table055`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table056`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table061`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table062`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table082`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table083`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table084`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table085`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table087`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table088`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table093`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table108`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table109`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table116`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table117`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table118`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table029`	union all   
SELECT  subject_id,  Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table030`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table057`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table058`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table059`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table078`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table079`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table080`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table103`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table110`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table111`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table112`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table113`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table127`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table128`	
)   	where Timestamp is not null	;	
###########################################################################
create table `your-project-id1234.R_TimeA.U_H_Time`  as  	
select distinct * from (
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table092`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table033`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table034`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table035`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table036`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table037`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table038`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table039`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table040`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table041`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table042`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table043`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table044`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table045`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table046`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table047`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table048`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table049`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table050`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table055`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table056`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table061`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table062`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table082`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table083`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table084`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table085`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table087`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table088`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table093`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table108`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table109`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table116`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table117`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table118`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table029`	union all   
SELECT  hadm_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table030`	
)   	where Timestamp is not null	;	
###########################################################################
create table `your-project-id1234.R_TimeA.U_T_Time`  as  	
select distinct * from (
SELECT  transfer_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table029`	union all   
SELECT  transfer_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table030`	
)   	where Timestamp is not null	;
###########################################################################	
create table `your-project-id1234.R_TimeA.U_I_Time`  as  	
select distinct * from (
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table092`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table069`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table071`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table072`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table073`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table074`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table076`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table077`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table086`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table090`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table091`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table094`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table095`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table096`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table097`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table098`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table099`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table100`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table101`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table102`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table104`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table105`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table107`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table114`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table115`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table119`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table120`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table121`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table122`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table123`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table124`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table125`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table126`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table129`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table130`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table033`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table034`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table035`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table036`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table037`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table038`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table039`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table040`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table041`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table042`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table043`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table044`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table045`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table046`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table047`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table048`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table049`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table050`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table055`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table056`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table061`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table062`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table082`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table083`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table084`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table085`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table087`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table088`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table093`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table108`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table109`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table116`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table117`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table118`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table057`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table058`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table059`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table078`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table079`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table080`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table103`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table110`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table111`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table112`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table113`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table127`	union all   
SELECT  stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table128`	
)   	where Timestamp is not null	;	
###########################################################################
create table `your-project-id1234.R_TimeA.U_SI_Time`  as  	
select distinct * from (
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table033`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table034`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table035`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table036`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table037`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table038`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table039`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table040`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table041`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table042`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table043`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table044`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table045`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table046`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table047`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table048`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table049`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table050`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table055`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table056`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table061`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table062`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table082`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table083`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table084`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table085`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table087`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table088`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table093`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table108`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table109`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table116`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table117`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table118`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table057`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table058`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table059`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table078`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table079`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table080`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table103`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table110`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table111`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table112`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table113`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table127`	union all   
SELECT  subject_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table128`	
)   	where Timestamp is not null	;	
###########################################################################
create table `your-project-id1234.R_TimeA.U_ST_Time`  as  	
select distinct * from (
SELECT  subject_id, transfer_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table029`	union all   
SELECT  subject_id, transfer_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table030`	
)   	where Timestamp is not null	;
###########################################################################	
create table `your-project-id1234.R_TimeA.U_HT_Time`  as  	
select distinct * from (
SELECT  hadm_id, transfer_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table029`	union all   
SELECT  hadm_id, transfer_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table030`	
)   	where Timestamp is not null	;	
###########################################################################
create table `your-project-id1234.R_TimeA.U_HI_Time`  as  	
select distinct * from (
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table092`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table033`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table034`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table035`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table036`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table037`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table038`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table039`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table040`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table041`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table042`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table043`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table044`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table045`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table046`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table047`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table048`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table049`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table050`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table055`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table056`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table061`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table062`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table082`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table083`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table084`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table085`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table087`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table088`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table093`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table108`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table109`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table116`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table117`	union all   
SELECT  hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table118`	
)   	where Timestamp is not null	;	
###########################################################################
create table `your-project-id1234.R_TimeA.U_SHI_Time`  as  	
select distinct * from (
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table033`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table034`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table035`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table036`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table037`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table038`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table039`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table040`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table041`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table042`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table043`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table044`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table045`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table046`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table047`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table048`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table049`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table050`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table055`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table056`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table061`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table062`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table082`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table083`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table084`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table085`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table087`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table088`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table093`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table108`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table109`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table116`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table117`	union all   
SELECT  subject_id, hadm_id, stay_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table118`	
)   	where Timestamp is not null	;	
###########################################################################
create table `your-project-id1234.R_TimeA.U_SHT_Time`  as  	
select distinct * from (
SELECT  subject_id, hadm_id, transfer_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table029`	union all   
SELECT  subject_id, hadm_id, transfer_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table030`	
)   	where Timestamp is not null	;	
###########################################################################
create table `your-project-id1234.R_TimeA.U_S_Date`  as  	
select distinct * from (
SELECT  subject_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table015`	union all   
SELECT  subject_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table016`	union all   
SELECT  subject_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table081`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table006`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table007`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table011`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table013`	union all   
SELECT  subject_id, Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table051`	
)   	where Timestamp is not null	;	
###########################################################################
create table `your-project-id1234.R_TimeA.U_SH_Date`  as  	
select distinct * from (
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table006`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table007`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table011`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table013`	union all   
SELECT  subject_id, hadm_id,Timestamp, Title      FROM     `your-project-id1234.R_TimeA.Table051`	
)   	where Timestamp is not null	;	
###########################################################################

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
