import os
from google.cloud import bigquery

symPath = '../gcKey/SNOMED_CT_Google_Cloud.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

###################################
################################### Go to SQL \ Query 10_SCT
###################################
# Perform a query.
query1 = f'''            

CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB01`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s0 , p0 , s1 , p1   FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s0 is not null AND s1 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB02`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s1 , p1 , s2 , p2    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s1 is not null AND s2 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB03`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s2 , p2 , s3 , p3    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s2 is not null AND s3 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB04`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s3 , p3 , s4 , p4    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s3 is not null AND s4 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB05`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s4 , p4 , s5 , p5    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s4 is not null AND s5 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB06`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s5 , p5 , s6 , p6    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s5 is not null AND s6 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB07`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s6 , p6 , s7 , p7    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s6 is not null AND s7 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB08`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s7 , p7 , s8 , p8    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s7 is not null AND s8 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB09`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s8 , p8 , s9 , p9    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s8 is not null AND s9 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB10`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s9 , p9 , s10 , p10    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s9 is not null AND s10 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB11`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s10 , p10 , s11 , p11    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s10 is not null AND s11 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB12`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s11 , p11 , s12 , p12    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s11 is not null AND s12 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB13`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s12 , p12 , s13 , p13    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s12 is not null AND s13 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB14`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s13 , p13 , s14 , p14    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s13 is not null AND s14 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB15`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s14 , p14 , s15 , p15    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s14 is not null AND s15 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB16`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s15 , p15 , s16 , p16    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s15 is not null AND s16 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB17`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s16 , p16 , s17 , p17    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s16 is not null AND s17 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB18`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s17 , p17 , s18 , p18    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s17 is not null AND s18 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB19`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s18 , p18 , s19 , p19    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s18 is not null AND s19 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB20`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s19 , p19 , s20 , p20    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s19 is not null AND s20 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB21`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s20 , p20 , s21 , p21    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s20 is not null AND s21 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB22`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s21 , p21 , s22 , p22    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s21 is not null AND s22 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB23`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s22 , p22 , s23 , p23    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s22 is not null AND s23 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB24`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s23 , p23 , s24 , p24    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s23 is not null AND s24 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB25`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s24 , p24 , s25 , p25    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s24 is not null AND s25 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB26`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s25 , p25 , s26 , p26    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s25 is not null AND s26 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB27`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s26 , p26 , s27 , p27    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s26 is not null AND s27 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB28`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s27 , p27 , s28 , p28    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s27 is not null AND s28 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB29`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s28 , p28 , s29 , p29    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s28 is not null AND s29 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB30`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s29 , p29 , s30 , p30    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s29 is not null AND s30 is not null ;


CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperB31`   AS 
SELECT DISTINCT * FROM (  
SELECT DISTINCT s30 , p30 , s31 , p31    FROM `snomed-ct-ml4219.K06_SCT.PaperB` 
)
WHERE  s30 is not null AND s31 is not null ;











'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
