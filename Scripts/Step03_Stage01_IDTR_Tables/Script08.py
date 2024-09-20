import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



#######################################################

create Table  `your-project-id1234.R_TimeA.Y_SH` as

(SELECT  distinct

a.subject_id,
a.hadm_id,
a.mini as mini_Time,
a.maxi as maxi_Time,
a.Title_mini as Title_mini_Time,
a.Title_maxi as Title_maxi_Time,

b.mini as mini_Date,
b.maxi as maxi_Date,
b.Title_mini as Title_mini_Date,
b.Title_maxi as Title_maxi_Date

FROM `your-project-id1234.R_TimeA.X_SH_Time` a
left join `your-project-id1234.R_TimeA.X_SH_Date` b
on
a.subject_id=b.subject_id
and
a.hadm_id=b.hadm_id)
;

#######################################################


create Table  `your-project-id1234.R_TimeA.Y_S` as

(SELECT  distinct

a.subject_id,
a.mini as mini_Time,
a.maxi as maxi_Time,
a.Title_mini as Title_mini_Time,
a.Title_maxi as Title_maxi_Time,

b.mini as mini_Date,
b.maxi as maxi_Date,
b.Title_mini as Title_mini_Date,
b.Title_maxi as Title_maxi_Date

FROM `your-project-id1234.R_TimeA.X_S_Time` a
left join `your-project-id1234.R_TimeA.X_S_Date` b
on
a.subject_id=b.subject_id)
;

#######################################################



create Table  `your-project-id1234.R_TimeA.Y_H` as

SELECT distinct * FROM `your-project-id1234.R_TimeA.X_H_Time` 
;

#######################################################




create Table  `your-project-id1234.R_TimeA.Y_T` as

SELECT distinct * FROM `your-project-id1234.R_TimeA.X_T_Time` 
;

#######################################################



create Table  `your-project-id1234.R_TimeA.Y_I` as

SELECT distinct * FROM `your-project-id1234.R_TimeA.X_I_Time` 
;

#######################################################





create Table  `your-project-id1234.R_TimeA.Y_SI` as

SELECT distinct * FROM `your-project-id1234.R_TimeA.X_SI_Time` 
;

#######################################################




create Table  `your-project-id1234.R_TimeA.Y_ST` as

SELECT distinct * FROM `your-project-id1234.R_TimeA.X_ST_Time` 
;

#######################################################




create Table  `your-project-id1234.R_TimeA.Y_HT` as

SELECT distinct * FROM `your-project-id1234.R_TimeA.X_HT_Time` 
;


#######################################################




create Table  `your-project-id1234.R_TimeA.Y_HI` as

SELECT distinct * FROM `your-project-id1234.R_TimeA.X_HI_Time` 
;


#######################################################




create Table  `your-project-id1234.R_TimeA.Y_SHI` as

SELECT distinct * FROM `your-project-id1234.R_TimeA.X_SHI_Time` 
;

#######################################################



create Table  `your-project-id1234.R_TimeA.Y_SHT` as

SELECT distinct * FROM `your-project-id1234.R_TimeA.X_SHT_Time` 
;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
