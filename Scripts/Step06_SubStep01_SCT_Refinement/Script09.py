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


update `snomed-ct-ml4219.K06_SCT.PaperB01` 
set p0=0
where p0 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB02` 
set p1=1
where p1 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB03` 
set p2=2
where p2 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB04` 
set p3=3
where p3 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB05` 
set p4=4
where p4 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB06` 
set p5=5
where p5 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB07` 
set p6=6
where p6 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB08` 
set p7=7
where p7 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB09` 
set p8=8
where p8 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB10` 
set p9=9
where p9 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB11` 
set p10=10
where p10 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB12` 
set p11=11
where p11 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB13` 
set p12=12
where p12 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB14` 
set p13=13
where p13 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB15` 
set p14=14
where p14 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB16` 
set p15=15
where p15 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB17` 
set p16=16
where p16 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB18` 
set p17=17
where p17 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB19` 
set p18=18
where p18 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB20` 
set p19=19
where p19 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB21` 
set p20=20
where p20 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB22` 
set p21=21
where p21 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB23` 
set p22=22
where p22 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB24` 
set p23=23
where p23 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB25` 
set p24=24
where p24 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB26` 
set p25=25
where p25 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB27` 
set p26=26
where p26 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB28` 
set p27=27
where p27 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB29` 
set p28=28
where p28 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB30` 
set p29=29
where p29 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB31` 
set p30=30
where p30 is null;

########################################################################

update `snomed-ct-ml4219.K06_SCT.PaperB01` 
set p1=1
where p1 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB02` 
set p2=2
where p2 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB03` 
set p3=3
where p3 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB04` 
set p4=4
where p4 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB05` 
set p5=5
where p5 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB06` 
set p6=6
where p6 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB07` 
set p7=7
where p7 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB08` 
set p8=8
where p8 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB09` 
set p9=9
where p9 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB10` 
set p10=10
where p10 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB11` 
set p11=11
where p11 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB12` 
set p12=12
where p12 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB13` 
set p13=13
where p13 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB14` 
set p14=14
where p14 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB15` 
set p15=15
where p15 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB16` 
set p16=16
where p16 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB17` 
set p17=17
where p17 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB18` 
set p18=18
where p18 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB19` 
set p19=19
where p19 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB20` 
set p20=20
where p20 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB21` 
set p21=21
where p21 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB22` 
set p22=22
where p22 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB23` 
set p23=23
where p23 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB24` 
set p24=24
where p24 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB25` 
set p25=25
where p25 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB26` 
set p26=26
where p26 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB27` 
set p27=27
where p27 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB28` 
set p28=28
where p28 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB29` 
set p29=29
where p29 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB30` 
set p30=30
where p30 is null;

update `snomed-ct-ml4219.K06_SCT.PaperB31` 
set p31=31
where p31 is null;






'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
