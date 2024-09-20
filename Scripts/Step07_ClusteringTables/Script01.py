import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create table `your-project-id1234.O_NonEvents_ClusData.Q01_Admissions` as
SELECT * FROM `your-project-id1234.S_Admission.1_Admissions` ;


create table `your-project-id1234.O_NonEvents_ClusData.Q01_icustay_detail` as
SELECT subject_id,hadm_id, stay_id, gender,  admission_age, race, dod, hospital_expire_flag FROM `your-project-id1234.S_Derived14.icustay_detail` ;


create table `your-project-id1234.O_NonEvents_ClusData.Q01_OMR` as
SELECT * FROM `your-project-id1234.S_OMR.1_OMR` ;

create table `your-project-id1234.O_NonEvents_ClusData.Q01_icd` as
SELECT distinct subject_id, MC_num, hadm_num, FROM `your-project-id1234.P_NonEvents_ICD3.icdCM6` ;



'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
