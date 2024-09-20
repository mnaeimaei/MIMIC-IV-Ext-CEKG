import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

CREATE TABLE  `your-project-id1234.N01_NDC.zzu21`   AS
SELECT * FROM `your-project-id1234.N01_NDC.u21` ;

####################################################################################################################################################################################

CREATE TABLE  `your-project-id1234.N01_NDC.zzu22`   AS
SELECT * FROM `your-project-id1234.N01_NDC.u22` ;

####################################################################################################################################################################################

CREATE TABLE  `your-project-id1234.N01_NDC.zzu23`   AS
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.z031`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.z033`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.z035`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.z037`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.z039`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.z041`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.z043`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.z044`  ;



####################################################################################################################################################################################

CREATE TABLE  `your-project-id1234.N01_NDC.zzu24`   AS
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.zz033`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.zz035`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.zz037`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.zz039`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.zz041`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.zz043`  
union all
SELECT eventID , medication_p , medication_e , Drug , product_description , formulary_drug_cd , Gsn , Ndc2 , product_code      FROM `your-project-id1234.N01_NDC.zz044`  ;

####################################################################################################################################################################################

CREATE TABLE  `your-project-id1234.N01_NDC.zzu25`   AS
SELECT * FROM `your-project-id1234.N01_NDC.u25` ;
'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
