import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

create table `your-project-id1234.N01_NDC.u1`   as
SELECT 
eventID, medication_p, medication_e, Drug, product_description, formulary_drug_cd, Gsn, Ndc, product_code
FROM `your-project-id1234.N01_NDC.t4` ;


        
create table `your-project-id1234.N01_NDC.u21`   as
SELECT * FROM `your-project-id1234.N01_NDC.u1` 
where Ndc is not null and  product_code is not null;       
        
create table `your-project-id1234.N01_NDC.u22`   as
SELECT * FROM `your-project-id1234.N01_NDC.u1` 
where Ndc is not null and  product_code is null;

create table `your-project-id1234.N01_NDC.u23`   as
SELECT * FROM `your-project-id1234.N01_NDC.u1` 
where Ndc is  null and  product_code is not null;

create table `your-project-id1234.N01_NDC.u24`   as
SELECT * FROM `your-project-id1234.N01_NDC.u1` 
where Ndc is  null and  product_code is null
and (
medication_p is not null or 
medication_e is not null or 
Drug is not null or 
product_description is not null )
;

create table `your-project-id1234.N01_NDC.u25`   as
SELECT * FROM `your-project-id1234.N01_NDC.u1` 
where Ndc is  null and product_code is null 
and
medication_p is null and 
medication_e is null and 
Drug is null and 
product_description is null 
;



'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
