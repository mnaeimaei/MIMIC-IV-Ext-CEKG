import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''

    create schema `your-project-id1234.M80_FBGT` ;
    create table `your-project-id1234.M80_FBGT.TabA1` as
    SELECT * FROM `your-project-id1234.N08_BG.first_day_bg_final` ;



    CREATE TABLE  `your-project-id1234.M80_FBGT.TabA2`   AS
    SELECT distinct * FROM (
    SELECT
    a.subject_id , b.hadm_id,a.stay_id , a.Timestamps , a.Type , a.lactate_min , a.lactate_max , a.ph_min , a.ph_max , a.so2_min , a.so2_max , a.po2_min , a.po2_max , a.pco2_min , a.pco2_max , a.aado2_min , a.aado2_max , a.aado2_calc_min , a.aado2_calc_max , a.pao2fio2ratio_min , a.pao2fio2ratio_max , a.baseexcess_min , a.baseexcess_max , a.bicarbonate_min , a.bicarbonate_max , a.totalco2_min , a.totalco2_max , a.hematocrit_min , a.hematocrit_max , a.hemoglobin_min , a.hemoglobin_max , a.carboxyhemoglobin_min , a.carboxyhemoglobin_max , a.methemoglobin_min , a.methemoglobin_max , a.temperature_min , a.temperature_max , a.chloride_min , a.chloride_max , a.calcium_min , a.calcium_max , a.glucose_min , a.glucose_max , a.potassium_min , a.potassium_max , a.sodium_min , a.sodium_max,
    From `your-project-id1234.M80_FBGT.TabA1`   as a
    LEFT JOIN `your-project-id1234.R_TimeD.SHI`   as b
    ON
    a.subject_id=b.subject_id AND a.stay_id=b.stay_id
    AND  a.Timestamps>=b.min AND a.Timestamps<=b.max
    )
    ;



    CREATE TABLE  `your-project-id1234.M80_FBGT.TabA3`   AS
    SELECT distinct
    subject_id, hadm_id, Timestamps, Type, lactate_min, lactate_max, ph_min, ph_max, so2_min, so2_max, po2_min, po2_max, pco2_min, pco2_max, aado2_min, aado2_max, aado2_calc_min, aado2_calc_max, pao2fio2ratio_min, pao2fio2ratio_max, baseexcess_min, baseexcess_max, bicarbonate_min, bicarbonate_max, totalco2_min, totalco2_max, hematocrit_min, hematocrit_max, hemoglobin_min, hemoglobin_max, carboxyhemoglobin_min, carboxyhemoglobin_max, methemoglobin_min, methemoglobin_max, temperature_min, temperature_max, chloride_min, chloride_max, calcium_min, calcium_max, glucose_min, glucose_max, potassium_min, potassium_max, sodium_min, sodium_max
    From `your-project-id1234.M80_FBGT.TabA2` ;



    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  Activity_Synonym STRING ;
    UPDATE `your-project-id1234.M80_FBGT.TabA3`
    SET Activity_Synonym ="FBGT"
    WHERE Activity_Synonym is null ;
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  Activity STRING ;
    UPDATE `your-project-id1234.M80_FBGT.TabA3`
    SET Activity ="First_Day_Blood_Gas_Test"
    WHERE Activity is null ;




    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f1 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f1="Type"
    where f1 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f2 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f2="lactate_min"
    where f2 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f3 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f3="lactate_max"
    where f3 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f4 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f4="ph_min"
    where f4 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f5 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f5="ph_max"
    where f5 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f6 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f6="so2_min"
    where f6 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f7 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f7="so2_max"
    where f7 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f8 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f8="po2_min"
    where f8 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f9 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f9="po2_max"
    where f9 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f10 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f10="pco2_min"
    where f10 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f11 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f11="pco2_max"
    where f11 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f12 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f12="aado2_min"
    where f12 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f13 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f13="aado2_max"
    where f13 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f14 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f14 ="aado2_calc_min"
    where f14 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f15 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f15 ="aado2_calc_max"
    where f15 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f16 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f16 ="pao2fio2ratio_min"
    where f16 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f17 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f17 ="pao2fio2ratio_max"
    where f17 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f18 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f18 ="baseexcess_min"
    where f18 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f19 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f19 ="baseexcess_max"
    where f19 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f20 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f20 ="bicarbonate_min"
    where f20 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f21 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f21 ="bicarbonate_max"
    where f21 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f22 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f22 ="totalco2_min"
    where f22 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f23 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f23 ="totalco2_max"
    where f23 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f24 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f24 ="hematocrit_min"
    where f24 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f25 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f25 ="hematocrit_max"
    where f25 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f26 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f26="hemoglobin_min"
    where f26 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f27 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f27="hemoglobin_max"
    where f27 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f28 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f28="carboxyhemoglobin_min"
    where f28 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f29 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f29="carboxyhemoglobin_max"
    where f29 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f30 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f30="methemoglobin_min"
    where f30 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f31 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f31="methemoglobin_max"
    where f31 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f32 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f32="temperature_min"
    where f32 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f33 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f33="temperature_max"
    where f33 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f34 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f34="chloride_min"
    where f34 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f35 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f35="chloride_max"
    where f35 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f36 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f36="calcium_min"
    where f36 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f37 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f37="calcium_max"
    where f37 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f38 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f38="glucose_min"
    where f38 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f39 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f39 ="glucose_max"
    where f39 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f40 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f40 ="potassium_min"
    where f40 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f41 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f41 ="potassium_max"
    where f41 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f42 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f42 ="sodium_min"
    where f42 is null;
    #################################################
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA3`
    ADD column  f43 STRING ;
    update `your-project-id1234.M80_FBGT.TabA3`
    set f43 ="sodium_max"
    where f43 is null;
    ################################################################

    create table `your-project-id1234.M80_FBGT.TabA4`   as
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f1 as feature , CAST(Type AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f2 as feature , CAST(lactate_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f3 as feature , CAST(lactate_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f4 as feature , CAST(ph_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f5 as feature , CAST(ph_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f6 as feature , CAST(so2_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f7 as feature , CAST(so2_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f8 as feature , CAST(po2_min AS STRING) as  value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f9 as feature , CAST(po2_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f10 as feature , CAST(pco2_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f11 as feature , CAST(pco2_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f12 as feature , CAST(aado2_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f13 as feature , CAST(aado2_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f14 as feature , CAST(aado2_calc_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f15 as feature , CAST(aado2_calc_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f16 as feature , CAST(pao2fio2ratio_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f17 as feature , CAST(pao2fio2ratio_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f18 as feature , CAST(baseexcess_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f19 as feature , CAST(baseexcess_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f20 as feature , CAST(bicarbonate_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f21 as feature , CAST(bicarbonate_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f22 as feature , CAST(totalco2_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f23 as feature , CAST(totalco2_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f24 as feature , CAST(hematocrit_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f25 as feature , CAST(hematocrit_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f26 as feature , CAST(hemoglobin_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f27 as feature , CAST(hemoglobin_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f28 as feature , CAST(carboxyhemoglobin_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f29 as feature , CAST(carboxyhemoglobin_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f30 as feature , CAST(methemoglobin_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f31 as feature , CAST(methemoglobin_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f32 as feature , CAST(temperature_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f33 as feature , CAST(temperature_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f34 as feature , CAST(chloride_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f35 as feature , CAST(chloride_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f36 as feature , CAST(calcium_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f37 as feature , CAST(calcium_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f38 as feature , CAST(glucose_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f39 as feature , CAST(glucose_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f40 as feature , CAST(potassium_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f41 as feature , CAST(potassium_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f42 as feature , CAST(sodium_min AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`
    union distinct
    select subject_id, hadm_id,   Timestamps, Activity,   Activity_Synonym, f43 as feature , CAST(sodium_max AS STRING) as value
    from  `your-project-id1234.M80_FBGT.TabA3`   ;
    #################################################
    alter table `your-project-id1234.M80_FBGT.TabA4`
    add column num INT64;
    update`your-project-id1234.M80_FBGT.TabA4`
    set num=1
    where num is null;



    CREATE TABLE `your-project-id1234.M80_FBGT.TabA5`  AS

    SELECT
    a.num,a.subject_id, a.hadm_id, a.Timestamps,
    b.RankN,
    a.Activity, a.Activity_Synonym, a.feature, a.value
    FROM `your-project-id1234.M80_FBGT.TabA4`  a
    LEFT   JOIN (
    SELECT
    num,subject_id, hadm_id, Timestamps,
    Row_number() over (partition by  num order by subject_id, hadm_id, Timestamps asc) as RankN
    FROM

    (SELECT   DISTINCT num,subject_id, hadm_id, Timestamps  FROM `your-project-id1234.M80_FBGT.TabA4`  )  ) b
    ON
    a.num = b.num AND a.subject_id = b.subject_id AND a.hadm_id = b.hadm_id AND a.Timestamps = b.Timestamps;




    ALTER TABLE `your-project-id1234.M80_FBGT.TabA5`
    ADD column  Activity_Value_ID STRING ;

    UPDATE `your-project-id1234.M80_FBGT.TabA5`
    SET Activity_Value_ID = concat("fbgt",RankN)
    WHERE Activity_Value_ID is null ;



    CREATE TABLE `your-project-id1234.M80_FBGT.TabA6`  AS
    SELECT
    subject_id, hadm_id, Timestamps,Activity, Activity_Synonym, Activity_Value_ID
    FROM `your-project-id1234.M80_FBGT.TabA5`  ;

    CREATE TABLE `your-project-id1234.M80_FBGT.TabA7`  AS
    SELECT
    Activity_Value_ID, Activity, feature as featureName, value as featureValue
    FROM `your-project-id1234.M80_FBGT.TabA5`  ;



    ALTER TABLE `your-project-id1234.M80_FBGT.TabA7`
    ADD column  Activity_Synonym STRING ;

    UPDATE `your-project-id1234.M80_FBGT.TabA7`
    SET Activity_Synonym = "FBGT"
    WHERE Activity_Synonym is null ;
    ALTER TABLE `your-project-id1234.M80_FBGT.TabA7`
    ADD column  num INT64 ;

    UPDATE `your-project-id1234.M80_FBGT.TabA7`
    SET num = 1
    WHERE num is null ;



    CREATE TABLE `your-project-id1234.M80_FBGT.TabA8`  AS

    SELECT
    a.num,a.Activity, a.Activity_Synonym, a.featureName, a.featureValue,
    b.RankN,
    a.Activity_Value_ID
    FROM `your-project-id1234.M80_FBGT.TabA7`  a
    LEFT   JOIN (
    SELECT
    num,Activity, Activity_Synonym, featureName, featureValue,
    Row_number() over (partition by  num order by Activity, Activity_Synonym, featureName, featureValue asc) as RankN
    FROM

    (SELECT   DISTINCT num,Activity, Activity_Synonym, featureName, featureValue  FROM `your-project-id1234.M80_FBGT.TabA7`  )  ) b
    ON
    a.num = b.num AND a.Activity = b.Activity AND a.Activity_Synonym = b.Activity_Synonym AND a.featureName = b.featureName AND a.featureValue = b.featureValue;




    CREATE TABLE `your-project-id1234.M80_FBGT.TabA9`  AS
    SELECT Activity_Value_ID, concat(Activity_Synonym,RankN) as Activity_Properties_ID
    FROM `your-project-id1234.M80_FBGT.TabA8`
    where RankN is not null
    order by Activity_Value_ID;



    CREATE TABLE `your-project-id1234.M80_FBGT.TabA10`  AS
    SELECT distinct
    Activity_Value_ID,
    STRING_AGG(Activity_Properties_ID,"," ORDER BY Activity_Properties_ID) Activity_Properties_ID_aggregation
    FROM `your-project-id1234.M80_FBGT.TabA9`
    GROUP BY Activity_Value_ID;



    CREATE TABLE `your-project-id1234.M80_FBGT.TabA11`  AS
    SELECT distinct * FROM (
    SELECT distinct
    a.subject_id , a.hadm_id , a.Timestamps , a.Activity , a.Activity_Synonym , a.Activity_Value_ID,
    b.Activity_Properties_ID_aggregation
    From `your-project-id1234.M80_FBGT.TabA6`   as a
    LEFT JOIN `your-project-id1234.M80_FBGT.TabA10`   as b
    ON
    a.Activity_Value_ID=b.Activity_Value_ID )        ;


    CREATE TABLE `your-project-id1234.M80_FBGT.TabA12`  AS
    SELECT distinct  * FROM (
    SELECT
    concat(Activity_Synonym,RankN) Activity_Properties_ID,  Activity , Activity_Synonym ,featureName , featureValue
    From `your-project-id1234.M80_FBGT.TabA8`
    where RankN is not null)    ;
    CREATE TABLE `your-project-id1234.M80_FBGT.TabA13`  AS
    SELECT distinct  * FROM (
    SELECT
    Activity_Value_ID,  Activity , Activity_Synonym ,featureName , featureValue
    From  `your-project-id1234.M80_FBGT.TabA7`  )    ;



    CREATE TABLE `your-project-id1234.M80_FBGT.TabA14`  AS
    SELECT distinct * FROM (
    SELECT
    a.Activity_Value_ID , a.Activity_Properties_ID,     b.Activity_Properties_ID_aggregation,
    From  `your-project-id1234.M80_FBGT.TabA9`    as a
    LEFT JOIN   `your-project-id1234.M80_FBGT.TabA10`    as b
    ON
    a.Activity_Value_ID=b.Activity_Value_ID )        ;




'''

QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
