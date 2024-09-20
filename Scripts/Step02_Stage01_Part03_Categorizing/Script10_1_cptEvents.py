import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
#print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create schema `your-project-id1234.S_cptEvent` ;

create table `your-project-id1234.S_cptEvent.1_cptEvents` as

select
           A.subject_id,
           A.hadm_id,
           A.chartdate,
           A.seq_num,
           A.hcpcs_cd,
           A.short_description,
           B.long_description,
           B.category,


from  `your-project-id1234.x_mimiciv_hosp.hcpcsevents` A
LEFT JOIN  `your-project-id1234.x_mimiciv_hosp.d_hcpcs` B
ON A.hcpcs_cd=B.code


####################

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
