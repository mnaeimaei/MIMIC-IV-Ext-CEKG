import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            




    CREATE TABLE  `your-project-id1234.R_TimeA.W_SH_Time`  as 
    (SELECT c.subject_id, c.hadm_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.subject_id, a.hadm_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT subject_id, hadm_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_SH_Time` 
    GROUP BY   subject_id, hadm_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_SH_Time`  b
    on a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_SH_Time`  d
    on c.subject_id=d.subject_id AND c.hadm_id=d.hadm_id and c.maxi=d.Timestamp)

;

###########################################

    CREATE TABLE  `your-project-id1234.R_TimeA.W_S_Time`  as 
    (SELECT c.subject_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.subject_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT subject_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_S_Time` 
    GROUP BY   subject_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_S_Time`  b
    on a.subject_id=b.subject_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_S_Time`  d
    on c.subject_id=d.subject_id and c.maxi=d.Timestamp)

;

###########################################


    CREATE TABLE  `your-project-id1234.R_TimeA.W_H_Time`  as 
    (SELECT c.hadm_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.hadm_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT hadm_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_H_Time` 
    GROUP BY   hadm_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_H_Time`  b
    on a.hadm_id=b.hadm_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_H_Time`  d
    on c.hadm_id=d.hadm_id and c.maxi=d.Timestamp)

;

###########################################


    CREATE TABLE  `your-project-id1234.R_TimeA.W_T_Time`  as 
    (SELECT c.transfer_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.transfer_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT transfer_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_T_Time` 
    GROUP BY   transfer_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_T_Time`  b
    on a.transfer_id=b.transfer_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_T_Time`  d
    on c.transfer_id=d.transfer_id and c.maxi=d.Timestamp)

;

###########################################

    CREATE TABLE  `your-project-id1234.R_TimeA.W_I_Time`  as 
    (SELECT c.stay_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.stay_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT stay_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_I_Time` 
    GROUP BY   stay_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_I_Time`  b
    on a.stay_id=b.stay_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_I_Time`  d
    on c.stay_id=d.stay_id and c.maxi=d.Timestamp)

;

    
###########################################


    CREATE TABLE  `your-project-id1234.R_TimeA.W_SI_Time`  as 
    (SELECT c.subject_id, c.stay_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.subject_id, a.stay_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT subject_id, stay_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_SI_Time` 
    GROUP BY   subject_id, stay_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_SI_Time`  b
    on a.subject_id=b.subject_id AND a.stay_id=b.stay_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_SI_Time`  d
    on c.subject_id=d.subject_id AND c.stay_id=d.stay_id and c.maxi=d.Timestamp)

;

###########################################


    CREATE TABLE  `your-project-id1234.R_TimeA.W_ST_Time`  as 
    (SELECT c.subject_id, c.transfer_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.subject_id, a.transfer_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT subject_id, transfer_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_ST_Time` 
    GROUP BY   subject_id, transfer_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_ST_Time`  b
    on a.subject_id=b.subject_id AND a.transfer_id=b.transfer_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_ST_Time`  d
    on c.subject_id=d.subject_id AND c.transfer_id=d.transfer_id and c.maxi=d.Timestamp)

;


###########################################

    CREATE TABLE  `your-project-id1234.R_TimeA.W_HT_Time`  as 
    (SELECT c.hadm_id, c.transfer_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.hadm_id, a.transfer_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT hadm_id, transfer_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_HT_Time` 
    GROUP BY   hadm_id, transfer_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_HT_Time`  b
    on a.hadm_id=b.hadm_id AND a.transfer_id=b.transfer_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_HT_Time`  d
    on c.hadm_id=d.hadm_id AND c.transfer_id=d.transfer_id and c.maxi=d.Timestamp)

;




###########################################

    CREATE TABLE  `your-project-id1234.R_TimeA.W_HI_Time`  as 
    (SELECT c.hadm_id, c.stay_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.hadm_id, a.stay_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT hadm_id, stay_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_HI_Time` 
    GROUP BY   hadm_id, stay_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_HI_Time`  b
    on a.hadm_id=b.hadm_id AND a.stay_id=b.stay_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_HI_Time`  d
    on c.hadm_id=d.hadm_id AND c.stay_id=d.stay_id and c.maxi=d.Timestamp)

;



###########################################
    CREATE TABLE  `your-project-id1234.R_TimeA.W_SHI_Time`  as 
    (SELECT c.subject_id, c.hadm_id, c.stay_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.subject_id, a.hadm_id, a.stay_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT subject_id, hadm_id, stay_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_SHI_Time` 
    GROUP BY   subject_id, hadm_id, stay_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_SHI_Time`  b
    on a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id AND a.stay_id=b.stay_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_SHI_Time`  d
    on c.subject_id=d.subject_id AND c.hadm_id=d.hadm_id AND c.stay_id=d.stay_id and c.maxi=d.Timestamp)

;


###########################################

    CREATE TABLE  `your-project-id1234.R_TimeA.W_SHT_Time`  as 
    (SELECT c.subject_id, c.hadm_id, c.transfer_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.subject_id, a.hadm_id, a.transfer_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT subject_id, hadm_id, transfer_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_SHT_Time` 
    GROUP BY   subject_id, hadm_id, transfer_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_SHT_Time`  b
    on a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id AND a.transfer_id=b.transfer_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_SHT_Time`  d
    on c.subject_id=d.subject_id AND c.hadm_id=d.hadm_id AND c.transfer_id=d.transfer_id and c.maxi=d.Timestamp)

;


###########################################

    CREATE TABLE  `your-project-id1234.R_TimeA.W_S_Date`  as 
    (SELECT c.subject_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.subject_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT subject_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_S_Date` 
    GROUP BY   subject_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_S_Date`  b
    on a.subject_id=b.subject_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_S_Date`  d
    on c.subject_id=d.subject_id and c.maxi=d.Timestamp)

;


###########################################

    CREATE TABLE  `your-project-id1234.R_TimeA.W_SH_Date`  as 
    (SELECT c.subject_id, c.hadm_id , c.mini, c.maxi, c.Title_mini, d.Title as Title_maxi
    from
    (SELECT a.subject_id, a.hadm_id , a.mini, a.maxi, b.Title as Title_mini
    from
    (SELECT subject_id, hadm_id, MIN(Timestamp) as mini, MAX(Timestamp) as maxi
    FROM `your-project-id1234.R_TimeA.V_SH_Date` 
    GROUP BY   subject_id, hadm_id   ) a
    
    
    left join `your-project-id1234.R_TimeA.V_SH_Date`  b
    on a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id and a.mini=b.Timestamp) c
    
    left join `your-project-id1234.R_TimeA.V_SH_Date`  d
    on c.subject_id=d.subject_id AND c.hadm_id=d.hadm_id and c.maxi=d.Timestamp)

;

    
###########################################



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
