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


       CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperC`   AS 
        SELECT DISTINCT * FROM   (
        SELECT DISTINCT * FROM (  
        SELECT DISTINCT s0 , p0 , s1 , p1   FROM `snomed-ct-ml4219.K06_SCT.PaperB01` 
        UNION ALL 
        SELECT DISTINCT s1 , p1 , s2 , p2    FROM `snomed-ct-ml4219.K06_SCT.PaperB02` 
        UNION ALL 
        SELECT DISTINCT s2 , p2 , s3 , p3    FROM `snomed-ct-ml4219.K06_SCT.PaperB03` 
        UNION ALL 
        SELECT DISTINCT s3 , p3 , s4 , p4    FROM `snomed-ct-ml4219.K06_SCT.PaperB04` 
        UNION ALL 
        SELECT DISTINCT s4 , p4 , s5 , p5    FROM `snomed-ct-ml4219.K06_SCT.PaperB05` 
        UNION ALL 
        SELECT DISTINCT s5 , p5 , s6 , p6    FROM `snomed-ct-ml4219.K06_SCT.PaperB06` 
        UNION ALL 
        SELECT DISTINCT s6 , p6 , s7 , p7    FROM `snomed-ct-ml4219.K06_SCT.PaperB07` 
        UNION ALL 
        SELECT DISTINCT s7 , p7 , s8 , p8    FROM `snomed-ct-ml4219.K06_SCT.PaperB08` 
        UNION ALL 
        SELECT DISTINCT s8 , p8 , s9 , p9    FROM `snomed-ct-ml4219.K06_SCT.PaperB09` 
        UNION ALL 
        SELECT DISTINCT s9 , p9 , s10 , p10    FROM `snomed-ct-ml4219.K06_SCT.PaperB10` 
        UNION ALL 
        SELECT DISTINCT s10 , p10 , s11 , p11    FROM `snomed-ct-ml4219.K06_SCT.PaperB11` 
        UNION ALL 
        SELECT DISTINCT s11 , p11 , s12 , p12    FROM `snomed-ct-ml4219.K06_SCT.PaperB12` 
        UNION ALL 
        SELECT DISTINCT s12 , p12 , s13 , p13    FROM `snomed-ct-ml4219.K06_SCT.PaperB13` 
        UNION ALL 
        SELECT DISTINCT s13 , p13 , s14 , p14    FROM `snomed-ct-ml4219.K06_SCT.PaperB14` 
        UNION ALL 
        SELECT DISTINCT s14 , p14 , s15 , p15    FROM `snomed-ct-ml4219.K06_SCT.PaperB15` 
        UNION ALL 
        SELECT DISTINCT s15 , p15 , s16 , p16    FROM `snomed-ct-ml4219.K06_SCT.PaperB16` 
        UNION ALL 
        SELECT DISTINCT s16 , p16 , s17 , p17    FROM `snomed-ct-ml4219.K06_SCT.PaperB17` 
        UNION ALL 
        SELECT DISTINCT s17 , p17 , s18 , p18    FROM `snomed-ct-ml4219.K06_SCT.PaperB18` 
        UNION ALL 
        SELECT DISTINCT s18 , p18 , s19 , p19    FROM `snomed-ct-ml4219.K06_SCT.PaperB19` 
        UNION ALL 
        SELECT DISTINCT s19 , p19 , s20 , p20    FROM `snomed-ct-ml4219.K06_SCT.PaperB20` 
        UNION ALL 
        SELECT DISTINCT s20 , p20 , s21 , p21    FROM `snomed-ct-ml4219.K06_SCT.PaperB21` 
        UNION ALL 
        SELECT DISTINCT s21 , p21 , s22 , p22    FROM `snomed-ct-ml4219.K06_SCT.PaperB22` 
        UNION ALL 
        SELECT DISTINCT s22 , p22 , s23 , p23    FROM `snomed-ct-ml4219.K06_SCT.PaperB23` 
        UNION ALL 
        SELECT DISTINCT s23 , p23 , s24 , p24    FROM `snomed-ct-ml4219.K06_SCT.PaperB24` 
        UNION ALL 
        SELECT DISTINCT s24 , p24 , s25 , p25    FROM `snomed-ct-ml4219.K06_SCT.PaperB25` 
        UNION ALL 
        SELECT DISTINCT s25 , p25 , s26 , p26    FROM `snomed-ct-ml4219.K06_SCT.PaperB26` 
        UNION ALL 
        SELECT DISTINCT s26 , p26 , s27 , p27    FROM `snomed-ct-ml4219.K06_SCT.PaperB27` 
        UNION ALL 
        SELECT DISTINCT s27 , p27 , s28 , p28    FROM `snomed-ct-ml4219.K06_SCT.PaperB28` 
        UNION ALL 
        SELECT DISTINCT s28 , p28 , s29 , p29    FROM `snomed-ct-ml4219.K06_SCT.PaperB29` 
        UNION ALL 
        SELECT DISTINCT s29 , p29 , s30 , p30    FROM `snomed-ct-ml4219.K06_SCT.PaperB30` 
        UNION ALL 
        SELECT DISTINCT s30 , p30 , s31 , p31    FROM `snomed-ct-ml4219.K06_SCT.PaperB31` 
       )
       WHERE  s0 is not null AND s1 is not null );
################################################################################
        CREATE TABLE  `snomed-ct-ml4219.K06_SCT.PaperD`   AS 
        SELECT DISTINCT * FROM (  
        SELECT DISTINCT s0, p0  FROM `snomed-ct-ml4219.K06_SCT.PaperC` 
        UNION ALL 
        SELECT DISTINCT s1, p1  FROM `snomed-ct-ml4219.K06_SCT.PaperC` 
       );








'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
