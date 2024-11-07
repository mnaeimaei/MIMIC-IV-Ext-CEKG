import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            

    CREATE TABLE  `your-project-id1234.N08_BG.first_day_bg_final`  as 
    SELECT subject_id , stay_id , Timestamps , Type , lactate_min , lactate_max , ph_min , ph_max , so2_min , so2_max , po2_min , po2_max , pco2_min , pco2_max , aado2_min , aado2_max , aado2_calc_min , aado2_calc_max , pao2fio2ratio_min , pao2fio2ratio_max , baseexcess_min , baseexcess_max , bicarbonate_min , bicarbonate_max , totalco2_min , totalco2_max , hematocrit_min , hematocrit_max , hemoglobin_min , hemoglobin_max , carboxyhemoglobin_min , carboxyhemoglobin_max , methemoglobin_min , methemoglobin_max , temperature_min , temperature_max , chloride_min , chloride_max , calcium_min , calcium_max , glucose_min , glucose_max , potassium_min , potassium_max , sodium_min , sodium_max 
    FROM `your-project-id1234.N08_BG.first_day_bg_time`  

union distinct
    SELECT subject_id , stay_id , Timestamps , Type , lactate_min , lactate_max , ph_min , ph_max , so2_min , so2_max , po2_min , po2_max , pco2_min , pco2_max , aado2_min , aado2_max , aado2_calc_min , aado2_calc_max , pao2fio2ratio_min , pao2fio2ratio_max , baseexcess_min , baseexcess_max , bicarbonate_min , bicarbonate_max , totalco2_min , totalco2_max , hematocrit_min , hematocrit_max , hemoglobin_min , hemoglobin_max , carboxyhemoglobin_min , carboxyhemoglobin_max , methemoglobin_min , methemoglobin_max , temperature_min , temperature_max , chloride_min , chloride_max , calcium_min , calcium_max , glucose_min , glucose_max , potassium_min , potassium_max , sodium_min , sodium_max 
    FROM `your-project-id1234.N08_BG.first_day_bg_art_time`  ;

    


'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
