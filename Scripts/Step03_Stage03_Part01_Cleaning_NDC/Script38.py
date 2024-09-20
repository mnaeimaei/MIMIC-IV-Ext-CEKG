import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.N01_NDC.zz31`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code,
        b.Ndc as Ndc2,
        From `your-project-id1234.N01_NDC.u24`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y04`   as b
        ON
        a.product_code=b.product_code )    
    ; 


CREATE TABLE  `your-project-id1234.N01_NDC.zz031`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz31` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.zz32`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz31` 
where Ndc2 is null;

#############################################################################################

        CREATE TABLE  `your-project-id1234.N01_NDC.zz33`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code ,
        b.Ndc as Ndc2,
        From `your-project-id1234.N01_NDC.zz32`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y64`   as b
        ON
        a.Gsn=b.Gsn )    
    ; 

CREATE TABLE  `your-project-id1234.N01_NDC.zz033`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz33` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.zz34`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz33` 
where Ndc2 is null;

#############################################################################################

        CREATE TABLE  `your-project-id1234.N01_NDC.zz35`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code,
        b.Ndc as Ndc2
        From `your-project-id1234.N01_NDC.zz34`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y54`   as b
        ON
        a.formulary_drug_cd=b.formulary_drug_cd )    
    ; 

CREATE TABLE  `your-project-id1234.N01_NDC.zz035`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz35` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.zz36`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz35` 
where Ndc2 is null;


#############################################################################################



#############################################################################################

        CREATE TABLE  `your-project-id1234.N01_NDC.zz37`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code,
        b.Ndc as Ndc2
        From `your-project-id1234.N01_NDC.zz36`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y14`   as b
        ON
        a.medication_p =b.medication_p  )    
    ; 


CREATE TABLE  `your-project-id1234.N01_NDC.zz037`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz37` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.zz38`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz37` 
where Ndc2 is null;

#############################################################################################  
            CREATE TABLE  `your-project-id1234.N01_NDC.zz39`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code,
        b.Ndc as Ndc2
        From `your-project-id1234.N01_NDC.zz38`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y24`   as b
        ON
        a.medication_e =b.medication_e  )    
    ; 


CREATE TABLE  `your-project-id1234.N01_NDC.zz039`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz39` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.zz40`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz39` 
where Ndc2 is null;


#############################################################################################


            CREATE TABLE  `your-project-id1234.N01_NDC.zz41`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code,
        b.Ndc as Ndc2
        From `your-project-id1234.N01_NDC.zz40`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y34`   as b
        ON
        a.Drug =b.Drug  )    
    ; 


CREATE TABLE  `your-project-id1234.N01_NDC.zz041`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz41` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.zz42`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz41` 
where Ndc2 is null;


#############################################################################################
            CREATE TABLE  `your-project-id1234.N01_NDC.zz43`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code,
        b.Ndc as Ndc2
        From `your-project-id1234.N01_NDC.zz42`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y44`   as b
        ON
        a.product_description =b.product_description  )    
    ; 



CREATE TABLE  `your-project-id1234.N01_NDC.zz043`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz43` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.zz044`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.zz43` 
where Ndc2 is null;









'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
