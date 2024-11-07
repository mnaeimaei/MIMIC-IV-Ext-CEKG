import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


        CREATE TABLE  `your-project-id1234.N08_BG.first_day_bg_time`   AS
        SELECT distinct a.subject_id , a.stay_id , a.Timestamps, a.lactate_min , a.lactate_max , a.ph_min , a.ph_max , a.so2_min , a.so2_max , a.po2_min , a.po2_max , a.pco2_min , a.pco2_max , a.aado2_min , a.aado2_max , a.aado2_calc_min , a.aado2_calc_max , a.pao2fio2ratio_min , a.pao2fio2ratio_max , a.baseexcess_min , a.baseexcess_max , a.bicarbonate_min , a.bicarbonate_max , a.totalco2_min , a.totalco2_max , a.hematocrit_min , a.hematocrit_max , a.hemoglobin_min , a.hemoglobin_max , a.carboxyhemoglobin_min , a.carboxyhemoglobin_max , a.methemoglobin_min , a.methemoglobin_max , a.temperature_min , a.temperature_max , a.chloride_min , a.chloride_max , a.calcium_min , a.calcium_max , a.glucose_min , a.glucose_max , a.potassium_min , a.potassium_max , a.sodium_min , a.sodium_max FROM (
        SELECT
        a.subject_id , a.stay_id , a.lactate_min , a.lactate_max , a.ph_min , a.ph_max , a.so2_min , a.so2_max , a.po2_min , a.po2_max , a.pco2_min , a.pco2_max , a.aado2_min , a.aado2_max , a.aado2_calc_min , a.aado2_calc_max , a.pao2fio2ratio_min , a.pao2fio2ratio_max , a.baseexcess_min , a.baseexcess_max , a.bicarbonate_min , a.bicarbonate_max , a.totalco2_min , a.totalco2_max , a.hematocrit_min , a.hematocrit_max , a.hemoglobin_min , a.hemoglobin_max , a.carboxyhemoglobin_min , a.carboxyhemoglobin_max , a.methemoglobin_min , a.methemoglobin_max , a.temperature_min , a.temperature_max , a.chloride_min , a.chloride_max , a.calcium_min , a.calcium_max , a.glucose_min , a.glucose_max , a.potassium_min , a.potassium_max , a.sodium_min , a.sodium_max,
        b.careunit , b.min , b.max,
        DATETIME_ADD(min, INTERVAL 1 DAY) as Timestamps
        From `your-project-id1234.N08_BG.first_day_bg`   as a
        LEFT JOIN `your-project-id1234.R_TimeC.SI`   as b
        ON
        a.subject_id=b.subject_id AND a.stay_id=b.stay_id 


        )  a  ;



        CREATE TABLE  `your-project-id1234.N08_BG.first_day_bg_art_time`   AS
        SELECT distinct a.subject_id , a.stay_id , a.Timestamps, a.lactate_min , a.lactate_max , a.ph_min , a.ph_max , a.so2_min , a.so2_max , a.po2_min , a.po2_max , a.pco2_min , a.pco2_max , a.aado2_min , a.aado2_max , a.aado2_calc_min , a.aado2_calc_max , a.pao2fio2ratio_min , a.pao2fio2ratio_max , a.baseexcess_min , a.baseexcess_max , a.bicarbonate_min , a.bicarbonate_max , a.totalco2_min , a.totalco2_max , a.hematocrit_min , a.hematocrit_max , a.hemoglobin_min , a.hemoglobin_max , a.carboxyhemoglobin_min , a.carboxyhemoglobin_max , a.methemoglobin_min , a.methemoglobin_max , a.temperature_min , a.temperature_max , a.chloride_min , a.chloride_max , a.calcium_min , a.calcium_max , a.glucose_min , a.glucose_max , a.potassium_min , a.potassium_max , a.sodium_min , a.sodium_max FROM (
        SELECT
        a.subject_id , a.stay_id , a.lactate_min , a.lactate_max , a.ph_min , a.ph_max , a.so2_min , a.so2_max , a.po2_min , a.po2_max , a.pco2_min , a.pco2_max , a.aado2_min , a.aado2_max , a.aado2_calc_min , a.aado2_calc_max , a.pao2fio2ratio_min , a.pao2fio2ratio_max , a.baseexcess_min , a.baseexcess_max , a.bicarbonate_min , a.bicarbonate_max , a.totalco2_min , a.totalco2_max , a.hematocrit_min , a.hematocrit_max , a.hemoglobin_min , a.hemoglobin_max , a.carboxyhemoglobin_min , a.carboxyhemoglobin_max , a.methemoglobin_min , a.methemoglobin_max , a.temperature_min , a.temperature_max , a.chloride_min , a.chloride_max , a.calcium_min , a.calcium_max , a.glucose_min , a.glucose_max , a.potassium_min , a.potassium_max , a.sodium_min , a.sodium_max,
        b.careunit , b.min , b.max,
        DATETIME_ADD(min, INTERVAL 1 DAY) as Timestamps
        From `your-project-id1234.N08_BG.first_day_bg_art`   as a
        LEFT JOIN `your-project-id1234.R_TimeC.SI`   as b
        ON
        a.subject_id=b.subject_id AND a.stay_id=b.stay_id 


        )  a  ;




########################################################################





'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
