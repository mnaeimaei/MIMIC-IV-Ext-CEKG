import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE  `your-project-id1234.N01_NDC.zzzu1`   AS

SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc , product_code 
FROM `your-project-id1234.N01_NDC.zzu21`  
union all

SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc , product_code 
FROM `your-project-id1234.N01_NDC.zzu22`  
union all

SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 as Ndc , product_code 
FROM `your-project-id1234.N01_NDC.zzu23`  
union all

SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 as Ndc , product_code 
FROM `your-project-id1234.N01_NDC.zzu24`  
union all

SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc , product_code 
FROM `your-project-id1234.N01_NDC.zzu25`  

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
