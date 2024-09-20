import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
#print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create table `your-project-id1234.S_Admission.2_Admissions` as
    select
           subject_id,
           hadm_id,
           edregtime,
           admission_location,
           admission_type,
           admit_provider_id,
           admittime,
           edouttime,
           hospital_expire_flag,
           dischtime,
           deathtime,         
           discharge_location

from  `your-project-id1234.x_mimiciv_hosp.admissions` 


'''


QUERY = (query1)


query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)

















