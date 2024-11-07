import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

    UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route_e = "PO"
    WHERE  route_e = "P.O."     ;
    
    UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route_e = "PO"
    WHERE  route_e = "po"     ;
    
    
    UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route_e = "PO/OG"
    WHERE  route = "PO/OG" and route_e = "OG"     ;
    
    UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route_e = "PO/NG"
    WHERE  route = "PO/NG" and route_e = "Po"     ;
    
    UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route_e = "PO/OG"
    WHERE  route = "PO/OG" and route_e = "PO"     ;
        
        
    UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route_e = "NG"
    WHERE  route = "NG" and route_e = " ng"     ;
    
    UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route_e = "NG/OG"
    WHERE  route = "NG/OG" and route_e = "OG"     ;
    
        UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route_e = "PO/NG"
    WHERE  route = "PO/NG" and route_e = "ng"     ;
    
    
    
        UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route_e = "NG/OG"
    WHERE  route = "NG/OG" and route_e = "NG"     ;
    
    
        UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route_e = "PO/NG"
    WHERE  route = "PO/NG" and route_e = "PO"     ;
    
        UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route_e = "PO/NG"
    WHERE  route = "PO/NG" and route_e = "NG"     ;
    
        UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route_e = "PO/NG"
    WHERE  route = "PO/NG" and route_e = " ng"     ;

UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "OG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "IJ"	 WHERE  route = "IJ" and route_e = "IM" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "IV"	 WHERE  route = "IV" and route_e = "SC" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PR" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "NG" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "IV" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "OG"	 WHERE  route = "OG" and route_e = "NG" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PR" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "NG" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "NG" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "IV" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO/NG"	 WHERE  route = "PO/NG" and route_e = "OGT" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PR" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "OGT" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "IM"	 WHERE  route = "IM" and route_e = "SC" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "PR" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "VG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PR" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "NG" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "NG" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "NG" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "IJ"	 WHERE  route = "IJ" and route_e = "SC" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "IV"	 WHERE  route = "IV" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "IM"	 WHERE  route = "IM" and route_e = "SC" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PR"	 WHERE  route = "PR" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "PR" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PR"	 WHERE  route = "PR" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PR"	 WHERE  route = "PO" and route_e = "PR" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO/NG"	 WHERE  route = "PO/NG" and route_e = "OGT" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "G TUBE" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "IV" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "NG" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "NG" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "OGT" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO/NG"	 WHERE  route = "PO/NG" and route_e = "NGT" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "NGT" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "G TUBE" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO/NG"	 WHERE  route = "PO/NG" and route_e = "PEG" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PR"	 WHERE  route = "PR" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "IV" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "OGT" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "G TUBE" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "SL" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "IV" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "NGT" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PR" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "OGT" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "OGT" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "IV"	 WHERE  route = "SC" and route_e = "IV" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO/NG"	 WHERE  route = "PO/NG" and route_e = "OGT" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO/NG"	 WHERE  route = "PO/NG" and route_e = "NGT" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "G TUBE" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PO" and route_e = "IV" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "NG" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "PO"	 WHERE  route = "PR" and route_e = "PO" ;
UPDATE `your-project-id1234.N01_NDC.t2`   	 SET route = "IM"	 WHERE  route = "SC" and route_e = "IM" ;

        UPDATE `your-project-id1234.N01_NDC.t2` 
    SET route=route_e
    WHERE   route is null   ;


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
