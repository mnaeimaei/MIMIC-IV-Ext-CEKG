import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



        ALTER TABLE  `your-project-id1234.O_NonEvents_ClusData.Q05_AdmissionPatient`   
        ADD COLUMN Died INTEGER;
        
        UPDATE  `your-project-id1234.O_NonEvents_ClusData.Q05_AdmissionPatient`   
        set Died=0
        where dod is not null;
        
        UPDATE   `your-project-id1234.O_NonEvents_ClusData.Q05_AdmissionPatient`   
        set Died=1
        where dod is null;
        
        ###############################################################
        
        ALTER TABLE  `your-project-id1234.O_NonEvents_ClusData.Q05_AdmissionPatient`   
        ADD COLUMN gender_Int INTEGER;
        
        UPDATE  `your-project-id1234.O_NonEvents_ClusData.Q05_AdmissionPatient`   
        set gender_Int=0
        where gender="F";
        
        UPDATE   `your-project-id1234.O_NonEvents_ClusData.Q05_AdmissionPatient`   
        set gender_Int=1
        where gender="M" ;


'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
