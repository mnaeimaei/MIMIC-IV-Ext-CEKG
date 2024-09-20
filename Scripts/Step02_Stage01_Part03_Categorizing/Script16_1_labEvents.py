import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
#print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            



create schema `your-project-id1234.S_labEvents` ;

create table `your-project-id1234.S_labEvents.1_labEvents` as

        SELECT
        a.subject_id, 
        a.hadm_id, 
        a.labevent_id, 
        a.specimen_id,
        a.itemid, 
        a.order_provider_id, 
        a.charttime, 
        a.storetime,
        a.value, 
        a.valuenum, 
        a.valueuom, 
        a.ref_range_lower, 
        a.ref_range_upper,
        a.flag, 
        a.priority, 
        a.comments,

        b.label,
        b.fluid, 
        b.category,
        From `your-project-id1234.x_mimiciv_hosp.labevents`   as a
        LEFT JOIN `your-project-id1234.x_mimiciv_hosp.d_labitems`   as b
        ON        a.itemid=b.itemid         ;


create table `your-project-id1234.S_labEvents.2_Hematology` as

SELECT 

subject_id	,
hadm_id	,
labevent_id	,
specimen_id	,
itemid	,
order_provider_id	,
charttime	,
storetime	,
value	,
valuenum	,
valueuom	,
ref_range_lower	,
ref_range_upper	,
flag	,
priority	,
comments	,
label	,
fluid	,

FROM `your-project-id1234.S_labEvents.1_labEvents` 
where category="Hematology";


create table `your-project-id1234.S_labEvents.2_Blood_Gas` as

SELECT 

subject_id	,
hadm_id	,
labevent_id	,
specimen_id	,
itemid	,
order_provider_id	,
charttime	,
storetime	,
value	,
valuenum	,
valueuom	,
ref_range_lower	,
ref_range_upper	,
flag	,
priority	,
comments	,
label	,
fluid	,

FROM `your-project-id1234.S_labEvents.1_labEvents` 
where category="Blood Gas";


create table `your-project-id1234.S_labEvents.2_Chemistry` as

SELECT 

subject_id	,
hadm_id	,
labevent_id	,
specimen_id	,
itemid	,
order_provider_id	,
charttime	,
storetime	,
value	,
valuenum	,
valueuom	,
ref_range_lower	,
ref_range_upper	,
flag	,
priority	,
comments	,
label	,
fluid	,

FROM `your-project-id1234.S_labEvents.1_labEvents` 
where category="Chemistry";

'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
