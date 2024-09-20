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



create Table  `your-project-id1234.R_TimeA.X_SH_Time` as

select 
subject_id,hadm_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_SH_Time` 
GROUP BY subject_id,hadm_id,mini,maxi

;


###########################################################################




create Table  `your-project-id1234.R_TimeA.X_S_Time` as

select 
subject_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_S_Time` 
GROUP BY subject_id,mini,maxi

;


###########################################################################





create Table  `your-project-id1234.R_TimeA.X_H_Time` as

select 
hadm_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_H_Time` 
GROUP BY hadm_id,mini,maxi

;


###########################################################################





create Table  `your-project-id1234.R_TimeA.X_T_Time` as

select 
transfer_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_T_Time` 
GROUP BY transfer_id,mini,maxi

;


###########################################################################




create Table  `your-project-id1234.R_TimeA.X_I_Time` as

select 
stay_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_I_Time` 
GROUP BY stay_id,mini,maxi

;



###########################################################################




create Table  `your-project-id1234.R_TimeA.X_SI_Time` as

select 
subject_id,stay_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_SI_Time` 
GROUP BY subject_id,stay_id,mini,maxi

;



###########################################################################




create Table  `your-project-id1234.R_TimeA.X_ST_Time` as

select 
subject_id,transfer_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_ST_Time` 
GROUP BY subject_id,transfer_id,mini,maxi

;



###########################################################################




create Table  `your-project-id1234.R_TimeA.X_HT_Time` as

select 
hadm_id,transfer_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_HT_Time` 
GROUP BY hadm_id,transfer_id,mini,maxi

;


###########################################################################





create Table  `your-project-id1234.R_TimeA.X_HI_Time` as

select 
hadm_id,stay_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_HI_Time` 
GROUP BY hadm_id,stay_id,mini,maxi

;


###########################################################################




create Table  `your-project-id1234.R_TimeA.X_SHI_Time` as

select 
subject_id,hadm_id,stay_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_SHI_Time` 
GROUP BY subject_id,hadm_id,stay_id,mini,maxi

;


###########################################################################





create Table  `your-project-id1234.R_TimeA.X_SHT_Time` as

select 
subject_id,hadm_id,transfer_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_SHT_Time` 
GROUP BY subject_id,hadm_id,transfer_id,mini,maxi

;


###########################################################################





create Table  `your-project-id1234.R_TimeA.X_S_Date` as

select 
subject_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_S_Date` 
GROUP BY subject_id,mini,maxi

;



###########################################################################



create Table  `your-project-id1234.R_TimeA.X_SH_Date` as

select 
subject_id,hadm_id,mini,maxi,
STRING_AGG(Title_mini , "/" ORDER BY Title_mini) as Title_mini, 
STRING_AGG(Title_maxi , "/" ORDER BY Title_maxi)  as Title_maxi
FROM  `your-project-id1234.R_TimeA.W_SH_Date` 
GROUP BY subject_id,hadm_id,mini,maxi

;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
