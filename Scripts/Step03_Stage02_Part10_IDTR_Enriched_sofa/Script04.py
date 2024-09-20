import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        CREATE TABLE  `your-project-id1234.N16_sofa.sofa_subject`   AS
        SELECT distinct * FROM (
        SELECT
        b.subject_id , a.stay_id , a.hr , a.starttime , a.endtime , a.pao2fio2ratio_novent , a.pao2fio2ratio_vent , a.rate_epinephrine , a.rate_norepinephrine , a.rate_dopamine , a.rate_dobutamine , a.meanbp_min , a.gcs_min , a.uo_24hr , a.bilirubin_max , a.creatinine_max , a.platelet_min , a.respiration , a.coagulation , a.liver , a.cardiovascular , a.cns , a.renal , a.respiration_24hours , a.coagulation_24hours , a.liver_24hours , a.cardiovascular_24hours , a.cns_24hours , a.renal_24hours , a.sofa_24hours,
        
        From `your-project-id1234.N16_sofa.sofa`   as a
        LEFT JOIN `your-project-id1234.R_TimeC.SI`   as b
        ON
        a.stay_id=b.stay_id 
        )    
    ; 
    
'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
