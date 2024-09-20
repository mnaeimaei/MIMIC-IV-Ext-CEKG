import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


create schema `your-project-id1234.S_ingredientEvents` ;

create table `your-project-id1234.S_ingredientEvents.1_ingredientEvent` as

        SELECT
        a.subject_id, 
        a.hadm_id, 
        a.stay_id, 
        a.caregiver_id, 
        a.starttime, 
        a.endtime, 
        a.storetime, 
        a.amount, 
        a.amountuom, 
        a.rate, 
        a.rateuom, 
        a.orderid, 
        a.linkorderid, 
        a.statusdescription, 
        a.originalamount, 
        a.originalrate,
        
        b.itemid, 
        b.label, 
        b.abbreviation, 
        b.category, 
        b.unitname, 
        b.param_type, 
        b.lownormalvalue, 
        b.highnormalvalue,
        
        From `your-project-id1234.x_mimiciv_icu.ingredientevents`   as a
        LEFT JOIN `your-project-id1234.x_mimiciv_icu.d_items`   as b
        ON
        a.itemid=b.itemid 
        
        
        ;




'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
