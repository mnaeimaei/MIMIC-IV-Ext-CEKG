import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        CREATE TABLE `your-project-id1234.N16_sofa.X_antibioticA3`  AS
        
        SELECT
        a.subject_id, a.hadm_id, a.Timestamps, a.stay_id,
        b.RankN,
        a.ab_id, a.antibiotic
        FROM `your-project-id1234.N16_sofa.X_antibioticA2`  a
        LEFT   JOIN (
        SELECT  
        subject_id, hadm_id, Timestamps, stay_id,
        Row_number() over (partition by  subject_id, hadm_id, Timestamps order by stay_id asc) as RankN
        FROM
        
        (SELECT   DISTINCT subject_id, hadm_id, Timestamps, stay_id  FROM `your-project-id1234.N16_sofa.X_antibioticA2`  )  ) b
        ON
        a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps AND a.stay_id = b.stay_id;


#############################################################################################


        CREATE TABLE `your-project-id1234.N16_sofa.X_cultureA3`  AS
        
        SELECT
        a.subject_id, a.hadm_id, a.Timestamps, a.stay_id,
        b.RankN,
        a.specimen, a.positive_culture
        FROM `your-project-id1234.N16_sofa.X_cultureA2`  a
        LEFT   JOIN (
        SELECT  
        subject_id, hadm_id, Timestamps, stay_id,
        Row_number() over (partition by  subject_id, hadm_id, Timestamps order by stay_id asc) as RankN
        FROM
        
        (SELECT   DISTINCT subject_id, hadm_id, Timestamps, stay_id  FROM `your-project-id1234.N16_sofa.X_cultureA2`  )  ) b
        ON
        a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps AND a.stay_id = b.stay_id;

#############################################################################################


        CREATE TABLE `your-project-id1234.N16_sofa.X_suspected_infectionA3`  AS
        
        SELECT
        a.subject_id, a.hadm_id, a.Timestamps, a.stay_id,
        b.RankN,
        a.suspected_infection
        FROM `your-project-id1234.N16_sofa.X_suspected_infectionA2`  a
        LEFT   JOIN (
        SELECT  
        subject_id, hadm_id, Timestamps, stay_id,
        Row_number() over (partition by  subject_id, hadm_id, Timestamps order by stay_id asc) as RankN
        FROM
        
        (SELECT   DISTINCT subject_id, hadm_id, Timestamps, stay_id  FROM `your-project-id1234.N16_sofa.X_suspected_infectionA2`  )  ) b
        ON
        a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps AND a.stay_id = b.stay_id;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
