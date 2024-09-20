import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

 
        CREATE TABLE  `your-project-id1234.N16_sofa.X_antibioticA2`   AS
        SELECT distinct * FROM (
        SELECT
        a.subject_id , b.stay_id , a.hadm_id , a.Timestamps , a.ab_id , a.antibiotic,
        From `your-project-id1234.N16_sofa.X_antibioticA1`   as a
        LEFT JOIN `your-project-id1234.R_TimeD.SHI`   as b
        ON
        a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id 
         AND  a.Timestamps>=b.min AND a.Timestamps<=b.max 

        )    
    ; 
    
    ##########################################################################
    
            CREATE TABLE  `your-project-id1234.N16_sofa.X_suspected_infectionA2`   AS
        SELECT distinct * FROM (
        SELECT
        a.subject_id , b.stay_id , a.hadm_id , a.Timestamps , a.suspected_infection,
        From `your-project-id1234.N16_sofa.X_suspected_infectionA1`   as a
        LEFT JOIN `your-project-id1234.R_TimeD.SHI`   as b
        ON
        a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id 
         AND  a.Timestamps>=b.min AND a.Timestamps<=b.max 

        )    
    ; 
    
        ##########################################################################
        
        
        CREATE TABLE  `your-project-id1234.N16_sofa.X_cultureA2`   AS
        SELECT distinct * FROM (
        SELECT
        a.subject_id , b.stay_id , a.hadm_id , a.Timestamps , a.specimen , a.positive_culture,
        From `your-project-id1234.N16_sofa.X_cultureA1`   as a
        LEFT JOIN `your-project-id1234.R_TimeD.SHI`   as b
        ON
        a.subject_id=b.subject_id AND a.hadm_id=b.hadm_id 
         AND  a.Timestamps>=b.min AND a.Timestamps<=b.max 

        )    
    ; 

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
