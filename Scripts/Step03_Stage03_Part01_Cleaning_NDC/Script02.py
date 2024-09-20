import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        UPDATE `your-project-id1234.N01_NDC.p1` 
        SET hadm_id = 0
        WHERE  hadm_id is null     ;
        
        UPDATE `your-project-id1234.N01_NDC.m1` 
        SET hadm_id = 0
        WHERE  hadm_id is null     ;
        
        UPDATE `your-project-id1234.N01_NDC.e1` 
        SET hadm_id = 0
        WHERE  hadm_id is null     ;
        
        
        UPDATE `your-project-id1234.N01_NDC.p1` 
        SET pharmacy_id = 0
        WHERE  pharmacy_id is null     ;
        
        UPDATE `your-project-id1234.N01_NDC.m1` 
        SET pharmacy_id = 0
        WHERE  pharmacy_id is null     ;
        
        UPDATE `your-project-id1234.N01_NDC.e1` 
        SET pharmacy_id = 0
        WHERE  pharmacy_id is null     ;
        
        
        UPDATE `your-project-id1234.N01_NDC.p1` 
        SET poe_id = "0"
        WHERE  poe_id is null     ;
        
        UPDATE `your-project-id1234.N01_NDC.m1` 
        SET poe_id = "0"
        WHERE  poe_id is null     ;
        
        UPDATE `your-project-id1234.N01_NDC.e1` 
        SET poe_id = "0"
        WHERE  poe_id is null     ;
        


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
