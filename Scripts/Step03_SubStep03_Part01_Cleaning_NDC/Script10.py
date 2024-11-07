import os
from google.cloud import bigquery
symPath='../gcKey/CEKG.json'
Realpath = os.path.realpath(symPath)
print(Realpath)


os.environ['GOOGLE_APPLICATION_CREDENTIALS']=Realpath

client = bigquery.Client()

# Perform a query.
query1 = f'''            


    create table `your-project-id1234.N01_NDC.Temp`   as
    Select * from `your-project-id1234.N01_NDC.t3`  ;
    
    ALTER TABLE `your-project-id1234.N01_NDC.Temp`  
    ADD column  c1 STRING ;
    
    UPDATE `your-project-id1234.N01_NDC.Temp`  
    SET c1 ="A"
    WHERE c1 is null ;
    
    create table `your-project-id1234.N01_NDC.Temp2`   as
    Select 
    Row_number() over (partition by c1 order by subject_id, hadm_id) as NewID, subject_id, hadm_id, pharmacy_id, poe_id, emar_id, emar_seq, poe_seq, order_provider_id, enter_provider_id, starttime, stoptime, charttime, scheduletime, storetime, event_txt, parent_field_ordinal, medication_p, medication_e, Drug, product_description, formulary_drug_cd, Gsn, Ndc, product_code, prod_strength, form_val_disp, form_unit_disp, form_rx, drug_type, route, administration_type, dose_val_rx, dose_unit_rx, frequency, basal_rate, one_hr_max, doses_per_24_hrs, duration, duration_interval
    from `your-project-id1234.N01_NDC.Temp`  ;
    
    ALTER TABLE `your-project-id1234.N01_NDC.Temp2`  
    ADD column  eventID STRING ;
    
    UPDATE `your-project-id1234.N01_NDC.Temp2`  
    SET eventID = concat("a",NewID)
    WHERE eventID is null ;
    
    create table `your-project-id1234.N01_NDC.t4`   as
    Select
    eventID, subject_id, hadm_id, pharmacy_id, poe_id, emar_id, emar_seq, poe_seq, order_provider_id, enter_provider_id, starttime, stoptime, charttime, scheduletime, storetime, event_txt, parent_field_ordinal, medication_p, medication_e, Drug, product_description, formulary_drug_cd, Gsn, Ndc, product_code, prod_strength, form_val_disp, form_unit_disp, form_rx, drug_type, route, administration_type, dose_val_rx, dose_unit_rx, frequency, basal_rate, one_hr_max, doses_per_24_hrs, duration, duration_interval
    from `your-project-id1234.N01_NDC.Temp2`  ;
    
    drop table `your-project-id1234.N01_NDC.Temp`  ;
    drop table `your-project-id1234.N01_NDC.Temp2`  ;
    
    UPDATE `your-project-id1234.N01_NDC.t4`  
    SET medication_p = "000"
    WHERE eventID = "a69446168" ;
    
        
    UPDATE `your-project-id1234.N01_NDC.t4`  
    SET Drug = "000"
    WHERE eventID = "a69446168" ;
    
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11574545" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11575301" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11574241" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11575461" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11573670" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11574787" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11573973" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11575285" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11575072" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11575243" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11573815" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11573728" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11573728" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11574865" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11575434" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11574343" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11574045" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11575053" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11575201" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11574214" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11574162" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11575735" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11574322" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11575079" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11574290" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11575782" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11573971" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11575620" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a11573863" ;

UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a46270730" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a27667866" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a26883355" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a52776587" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a19225869" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a19497766" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a63661308" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a44946006" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a25316028" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a17656146" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a6275963" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a26314698" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a49222748" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a65552433" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a66411043" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a39980292" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a21068506" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a12161270" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a49540786" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a16094543" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a15495064" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a47144857" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a52777072" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a64183552" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a53110446" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a26905585" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a46446448" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a12283951" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a60562806" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET product_description = "000"     WHERE eventID = "a15142716" ;

UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677151" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11559762" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11563477" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2374482" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6873542" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510249" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6873354" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509860" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43426619" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2372060" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575715" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29162104" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618497" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11564669" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509605" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510260" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674368" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2376467" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921691" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871815" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509003" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675201" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510442" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677509" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677220" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675958" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575992" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509419" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575556" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6873143" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675824" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509758" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43428137" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923998" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32924890" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29166702" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14895149" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427596" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11559580" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509594" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510195" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676258" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675875" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923942" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510301" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6873314" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675591" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575253" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675778" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894857" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43428683" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32926375" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43429142" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510444" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618701" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2371756" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618634" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923718" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920555" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32924450" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618544" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677080" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871397" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32924534" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674922" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510421" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921578" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619067" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14893448" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46576058" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509964" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509192" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921944" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619951" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2371800" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921884" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575479" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923980" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a63295538" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677135" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14893207" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981754" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619763" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675884" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920880" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675283" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510163" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871595" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619976" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29166279" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29154505" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427671" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509169" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923695" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871596" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619789" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29162417" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14895114" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923391" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39655318" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50980871" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29166974" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46576002" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921433" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509729" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39656217" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677060" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871004" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509502" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922273" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922000" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a5062127" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575619" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32925596" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674725" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981292" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2374383" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39654693" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510169" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32924828" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675390" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a63293642" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32922720" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675544" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675285" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510010" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894728" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2378310" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894788" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575927" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6873261" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619493" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2373254" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871834" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2377392" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2373205" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43426546" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509666" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11561792" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14893378" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922594" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674330" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29165874" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510064" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29156432" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2372248" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618339" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32926118" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a69400105" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676029" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871413" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2377167" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6873339" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510507" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2376352" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923815" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509742" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921524" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29161970" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922045" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981598" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2377453" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50980591" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427655" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922560" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871965" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50980683" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872117" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43428353" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29161519" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894777" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677605" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676358" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a63294418" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675422" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675899" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2382090" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675247" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2378636" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a66208541" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11562273" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509710" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50980656" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675850" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2372213" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676164" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510011" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575492" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2376256" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2372280" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510445" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676579" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675304" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2381487" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872556" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43428788" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427860" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921320" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a63294416" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32926363" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2372656" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29154750" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46574547" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509247" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2378387" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677289" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674431" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17508853" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50982088" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427681" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509816" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981549" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675360" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920779" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575626" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920770" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921452" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894592" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14893521" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872284" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674662" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922621" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510201" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2375362" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11562924" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39654616" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11562947" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2376925" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575910" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676051" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872913" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43428705" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981246" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921945" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676765" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871163" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872400" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921897" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675144" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43428079" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29160760" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6873540" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509167" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575883" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619970" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981052" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920872" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11559949" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46574701" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29158282" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32922539" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674435" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510385" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894726" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921466" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676617" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a63293540" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509136" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619885" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29160561" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675184" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510185" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923918" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14895103" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920730" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675246" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575340" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894833" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981105" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921228" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29157206" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894182" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677306" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675456" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11563150" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922166" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2381502" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676814" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510208" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32926301" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43426759" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6870934" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894613" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32925406" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43426699" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922132" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2382015" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510420" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2371787" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674729" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676853" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43429184" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427067" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921702" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872141" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29167512" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675450" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46574938" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575978" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923304" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2380486" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675281" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2375079" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29167938" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11564677" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674955" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981567" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29159168" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920595" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894018" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43428389" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11561499" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29166839" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509844" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677015" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920620" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32925561" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32925164" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29162727" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32924248" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922199" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981649" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674717" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32925129" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618555" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a33609352" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11565847" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29162213" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981713" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871899" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32925903" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29158696" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676793" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32924193" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2371830" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677710" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29154244" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29168294" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50980992" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29163644" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17508900" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922185" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29166150" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677047" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427421" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871207" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43426613" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575413" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29168439" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32922399" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29155924" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677034" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2380220" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14893181" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a63295386" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575549" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43426928" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48620106" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981547" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920500" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6873031" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6873176" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43426787" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981557" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427414" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29154412" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11562672" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509362" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920714" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922406" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a33608734" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32922551" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923825" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872061" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17508797" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427694" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29165015" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510416" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2380373" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894596" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427721" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29156956" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618684" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676404" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11561745" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676710" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619412" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29157466" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43426798" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43428156" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922274" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2380439" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675562" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50980799" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922031" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509643" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871536" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675186" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29168222" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43429204" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29164537" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675292" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2373053" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619660" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48620070" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619472" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921310" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676309" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509462" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618765" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921450" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29158036" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509262" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575020" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14893589" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2377125" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29156253" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618854" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11561876" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46576028" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29167737" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2380998" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921543" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675462" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509272" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677291" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509474" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920970" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29168255" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427939" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29165234" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2382000" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32924251" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923795" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509861" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29165749" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676762" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a33606244" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32926620" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6870866" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510255" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a63295036" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921196" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46576116" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922381" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894791" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14893687" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872332" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427490" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981032" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677633" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14893741" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43428507" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32922483" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676480" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46574826" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a66208463" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29154421" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618531" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2378755" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894300" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674924" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894308" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29157648" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29154646" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11562769" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427304" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2380012" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872106" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981837" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509101" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2371860" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43428722" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920774" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894953" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48620080" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2372379" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17508988" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922493" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921314" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676868" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921794" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894073" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894012" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921502" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871046" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32925539" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11561794" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575763" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29159457" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32922227" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29158980" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575852" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43426722" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676359" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922430" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43428636" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894949" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2377916" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46574744" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894325" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2381652" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32922535" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575593" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427088" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921270" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677064" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674708" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575913" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981663" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619903" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a63293597" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618452" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510051" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50980659" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676873" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11561667" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674704" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618712" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872312" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50982130" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921767" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2377893" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619071" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2377727" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17508702" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921109" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29154406" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872017" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575150" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981513" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32926068" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43426541" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509549" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575991" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922462" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871419" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11565330" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619770" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509278" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575628" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872510" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923306" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2379092" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674434" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677423" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2380522" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575310" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427204" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43429171" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11559596" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a33607941" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676696" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32922398" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17508934" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a46575013" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2675509" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509416" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29166657" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674818" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509451" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922224" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2382089" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981399" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619380" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618816" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920540" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2375526" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619755" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674276" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32922063" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29166469" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619189" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32924047" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29157074" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14893264" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2379860" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2674496" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29168259" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6870882" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676213" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2375787" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a65408836" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14893320" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50980585" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2381174" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509695" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50980613" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619511" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32923576" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871447" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6872681" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871047" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920926" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2380436" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676049" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a11560925" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17508704" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43427961" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a43428643" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a6871426" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48619342" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677661" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2380062" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894254" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29156836" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2676048" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32922790" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39920818" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981309" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2381849" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17510376" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17509608" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32925516" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39921405" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2378290" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a50981443" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2677269" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a32926013" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a39922496" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a14894837" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a29155415" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2375427" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a48618556" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a17508974" ;
UPDATE `your-project-id1234.N01_NDC.t4`  SET formulary_drug_cd = "000"     WHERE eventID = "a2380013" ;
    
    

UPDATE `your-project-id1234.N01_NDC.t4`  
SET Gsn = "000"     
where Gsn<>"___" and Gsn  not like "%0%" ;


UPDATE `your-project-id1234.N01_NDC.t4` 
SET Ndc = NULL
WHERE  Ndc = "0"  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET Ndc = NULL
WHERE  Ndc = " "  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET Ndc = NULL
WHERE  Ndc = ""  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET Ndc = NULL
WHERE  Ndc = "000"  ;

        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET medication_p = NULL
WHERE  medication_p = ""  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET medication_p = NULL
WHERE  medication_p = "000"  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET medication_p = NULL
WHERE  medication_p = "*NF*"  ;


UPDATE `your-project-id1234.N01_NDC.t4` 
SET medication_p = NULL
WHERE  medication_p = "*NF"  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET medication_p = NULL
WHERE  medication_p = "*nf"  ;


UPDATE `your-project-id1234.N01_NDC.t4` 
SET medication_p = NULL
WHERE  medication_p = "*nf* "  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET medication_p = NULL
WHERE  medication_p = "1"  ;


UPDATE `your-project-id1234.N01_NDC.t4` 
SET medication_p = NULL
WHERE  medication_p = "2"  ;


UPDATE `your-project-id1234.N01_NDC.t4` 
SET medication_p = NULL
WHERE  medication_p = "3"  ;



UPDATE `your-project-id1234.N01_NDC.t4` 
SET product_code = NULL
WHERE  product_code = "___"  ;


UPDATE `your-project-id1234.N01_NDC.t4` 
SET product_code = NULL
WHERE  product_code = "000"  ;

        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET medication_e = NULL
WHERE  medication_e = "___"  ;



UPDATE `your-project-id1234.N01_NDC.t4` 
SET medication_e = NULL
WHERE  medication_e = "0"  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET medication_e = NULL
WHERE  medication_e = "000"  ;



UPDATE `your-project-id1234.N01_NDC.t4` 
SET Drug = NULL
WHERE  Drug = ""  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET Drug = NULL
WHERE  Drug = "000"  ;

        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET Drug = NULL
WHERE  Drug = "___"  ;

        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET Drug = NULL
WHERE  Drug = "0"  ;


        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET Drug = NULL
WHERE  Drug = "*NF"  ;


        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET Drug = NULL
WHERE  Drug = "*nf"  ;


        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET Drug = NULL
WHERE  Drug = "*nf* "  ;



        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET Drug = NULL
WHERE  Drug = "*nf*"  ;



        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET Drug = NULL
WHERE  Drug = "1"  ;


        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET Drug = NULL
WHERE  Drug = "2"  ;





UPDATE `your-project-id1234.N01_NDC.t4` 
SET product_description = NULL
WHERE  product_description = ""  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET product_description = NULL
WHERE  product_description = " "  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET product_description = NULL
WHERE  product_description = "000"  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET product_description = NULL
WHERE  product_description = "*NF"  ;


UPDATE `your-project-id1234.N01_NDC.t4` 
SET product_description = NULL
WHERE  product_description = "*nf"  ;


UPDATE `your-project-id1234.N01_NDC.t4` 
SET product_description = NULL
WHERE  product_description = "*nf* "  ;



UPDATE `your-project-id1234.N01_NDC.t4` 
SET product_description = NULL
WHERE  product_description = "*nf*"  ;


UPDATE `your-project-id1234.N01_NDC.t4` 
SET product_description = NULL
WHERE  product_description = "1"  ;



UPDATE `your-project-id1234.N01_NDC.t4`   
SET product_description = NULL
WHERE  product_description = "2"  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET formulary_drug_cd = NULL
WHERE  formulary_drug_cd = "000"  ;

UPDATE `your-project-id1234.N01_NDC.t4` 
SET formulary_drug_cd = NULL
WHERE  formulary_drug_cd = "___"  ;


UPDATE `your-project-id1234.N01_NDC.t4` 
SET formulary_drug_cd = NULL
WHERE  formulary_drug_cd = "0"  ;


UPDATE `your-project-id1234.N01_NDC.t4` 
SET formulary_drug_cd = NULL
WHERE  formulary_drug_cd = "ever10"  ;




        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET Gsn = NULL
WHERE  Gsn = ""  ;

        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET Gsn = NULL
WHERE  Gsn = "___"  ;


        
UPDATE `your-project-id1234.N01_NDC.t4` 
SET Gsn = NULL
WHERE  Gsn = "000"  ;
    
    

        
        
        
        
'''


QUERY = (query1)

query_job = client.query(QUERY)  # API request
print(query_job)



for row in query_job.result():
    print(row)
