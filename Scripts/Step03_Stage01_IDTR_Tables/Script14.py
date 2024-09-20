import os
from google.cloud import bigquery

symPath = '../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

        update `your-project-id1234.R_TimeB.SHI_2`   
        set first_careunit = "ICU"
        where first_careunit is null;
        
        update `your-project-id1234.R_TimeB.SHI_2`   
        set last_careunit = "ICU"
        where last_careunit is null;
        

        update `your-project-id1234.R_TimeB.SI_2`   
        set first_careunit = "ICU"
        where first_careunit is null;
        
        update `your-project-id1234.R_TimeB.SI_2`   
        set last_careunit = "ICU"
        where last_careunit is null;
        
        
        update `your-project-id1234.R_TimeB.I_2`   
        set first_careunit = "ICU"
        where first_careunit is null;
        
        update `your-project-id1234.R_TimeB.I_2`   
        set last_careunit = "ICU"
        where last_careunit is null;
        
        
        update `your-project-id1234.R_TimeB.HI_2`   
        set first_careunit = "ICU"
        where first_careunit is null;
        
        update `your-project-id1234.R_TimeB.HI_2`   
        set last_careunit = "ICU"
        where last_careunit is null;
        


#################################



        alter table `your-project-id1234.R_TimeB.SHI_2`   
        add column careunit STRING;
        
        alter table `your-project-id1234.R_TimeB.SI_2`   
        add column careunit STRING;
        
        alter table `your-project-id1234.R_TimeB.I_2`   
        add column careunit STRING;
        
        alter table `your-project-id1234.R_TimeB.HI_2`   
        add column careunit STRING;
        
        ################################################################
        
        update `your-project-id1234.R_TimeB.SHI_2`   
        set careunit = first_careunit
        where first_careunit=last_careunit;
        
        update `your-project-id1234.R_TimeB.SHI_2`   
        set careunit = concat("first:",first_careunit,", last:",last_careunit)
        where first_careunit<>last_careunit;
        
        
        update `your-project-id1234.R_TimeB.SI_2`   
        set careunit = first_careunit
        where first_careunit=last_careunit;
        
        update `your-project-id1234.R_TimeB.SI_2`   
        set careunit = concat("first:",first_careunit,", last:",last_careunit)
        where first_careunit<>last_careunit;
        
        
        update `your-project-id1234.R_TimeB.I_2`   
        set careunit = first_careunit
        where first_careunit=last_careunit;
        
        update `your-project-id1234.R_TimeB.I_2`   
        set careunit = concat("first:",first_careunit,", last:",last_careunit)
        where first_careunit<>last_careunit;
        
        
        update `your-project-id1234.R_TimeB.HI_2`   
        set careunit = first_careunit
        where first_careunit=last_careunit;
        
        update `your-project-id1234.R_TimeB.HI_2`   
        set careunit = concat("first:",first_careunit,", last:",last_careunit)
        where first_careunit<>last_careunit;
        
        
        

'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)

for row in query_job.result():
    print(row)
