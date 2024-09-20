import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        CREATE TABLE  `your-project-id1234.N01_NDC.z31`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code,
        b.Ndc as Ndc2,
        From `your-project-id1234.N01_NDC.u23`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y04`   as b
        ON
        a.product_code=b.product_code )    
    ; 
    
    
CREATE TABLE  `your-project-id1234.N01_NDC.z031`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z31` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.z32`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z31` 
where Ndc2 is null;

#############################################################################################

        CREATE TABLE  `your-project-id1234.N01_NDC.z33`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code ,
        b.Ndc as Ndc2,
        From `your-project-id1234.N01_NDC.z32`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y64`   as b
        ON
        a.Gsn=b.Gsn )    
    ; 

CREATE TABLE  `your-project-id1234.N01_NDC.z033`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z33` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.z34`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z33` 
where Ndc2 is null;

#############################################################################################

        CREATE TABLE  `your-project-id1234.N01_NDC.z35`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code,
        b.Ndc as Ndc2
        From `your-project-id1234.N01_NDC.z34`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y54`   as b
        ON
        a.formulary_drug_cd=b.formulary_drug_cd )    
    ; 
    
CREATE TABLE  `your-project-id1234.N01_NDC.z035`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z35` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.z36`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z35` 
where Ndc2 is null;
    
    
#############################################################################################



#############################################################################################

        CREATE TABLE  `your-project-id1234.N01_NDC.z37`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code,
        b.Ndc as Ndc2
        From `your-project-id1234.N01_NDC.z36`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y14`   as b
        ON
        a.medication_p =b.medication_p  )    
    ; 
    

CREATE TABLE  `your-project-id1234.N01_NDC.z037`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z37` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.z38`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z37` 
where Ndc2 is null;

#############################################################################################  
            CREATE TABLE  `your-project-id1234.N01_NDC.z39`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code,
        b.Ndc as Ndc2
        From `your-project-id1234.N01_NDC.z38`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y24`   as b
        ON
        a.medication_e =b.medication_e  )    
    ; 
    
    
CREATE TABLE  `your-project-id1234.N01_NDC.z039`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z39` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.z40`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z39` 
where Ndc2 is null;


#############################################################################################
    
    
            CREATE TABLE  `your-project-id1234.N01_NDC.z41`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code,
        b.Ndc as Ndc2
        From `your-project-id1234.N01_NDC.z40`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y34`   as b
        ON
        a.Drug =b.Drug  )    
    ; 
    

CREATE TABLE  `your-project-id1234.N01_NDC.z041`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z41` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.z42`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z41` 
where Ndc2 is null;


#############################################################################################
            CREATE TABLE  `your-project-id1234.N01_NDC.z43`   AS
        SELECT * FROM (
        SELECT
        a.eventID , a.medication_p , a.medication_e , a.Drug , a.product_description , a.formulary_drug_cd , a.Gsn , a.Ndc , a.product_code,
        b.Ndc as Ndc2
        From `your-project-id1234.N01_NDC.z42`   as a
        LEFT JOIN `your-project-id1234.N01_NDC.y44`   as b
        ON
        a.product_description =b.product_description  )    
    ; 
    
    
    
CREATE TABLE  `your-project-id1234.N01_NDC.z043`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z43` 
where Ndc2 is not null;

CREATE TABLE  `your-project-id1234.N01_NDC.z044`   AS
SELECT * FROM  `your-project-id1234.N01_NDC.z43` 
where Ndc2 is null;









'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
