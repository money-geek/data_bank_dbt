{{ config(materialized='table') }}

select n.* ,  ( o.state_pop_wt_rate_mandatory +  o.state_pop_wt_rate_bi_pd + o.state_pop_wt_rate_comp_coll +   o.state_pop_wt_rate_umbi+ o.state_pop_wt_rate_fees )as state_pop_wt_rate_core,
( ( o.state_pop_wt_rate_mandatory +  o.state_pop_wt_rate_bi_pd + o.state_pop_wt_rate_comp_coll +   o.state_pop_wt_rate_umbi+ o.state_pop_wt_rate_fees ) -state_annualpremium)/  ( o.state_pop_wt_rate_mandatory +  o.state_pop_wt_rate_bi_pd + o.state_pop_wt_rate_comp_coll +   o.state_pop_wt_rate_umbi+ o.state_pop_wt_rate_fees ) as change_premium
   
from {{ ref('ins_auto_pop_wt_new_premiums') }} n 
left join
    (select a.state,
        a.state_code,
        a.provider,
        a.age,
        a.gender,
        a.marital_status,
        a.drv2_age,
        a.drv2_gender,
        a.drv2_marital_status,
        a.drv3_age,
        a.drv3_gender,
        a.drv3_marital_status,
        a.vehicles,
        a.driving_record_violations,
        a.insurancescore_alignment ,
        a.coverage_level,
        a.number_incidents,
        a.number_accidents,
        a.number_duis,
        a.number_speedings,
        a.model_year,
        a.make,
        a.model,
        a.use,
        a.commute,
        a.annualmileage,
        a.ownership,
        a.residence_type,
        a.residence_occupancy,
        a.property_policy_type,
        a.bi_pd_limit,
        a.umbi_limit,
        a.comp_coll_deductible,
        a.is_user_a_veteran,
        avg(coalesce(a.state_pop_wt_rate_mandatory,0)) AS state_pop_wt_rate_mandatory,
        avg(coalesce(a.state_pop_wt_rate_bi_pd,0)) AS state_pop_wt_rate_bi_pd,
        avg(coalesce(a.state_pop_wt_rate_comp_coll,0)) AS state_pop_wt_rate_comp_coll,
        avg(coalesce(a.state_pop_wt_rate_umbi,0)) AS state_pop_wt_rate_umbi,
        avg(coalesce(a.state_pop_wt_rate_fees,0)) AS state_pop_wt_rate_fees
    from ins_auto_master_table_state_2022 a
    group by a.state, a.state_code, a.provider, a.age, a.gender, a.marital_status, a.drv2_age, a.drv2_gender, a.drv2_marital_status, a.drv3_age, a.drv3_gender, 
    a.drv3_marital_status, a.vehicles, a.driving_record_violations, a.insurancescore_alignment, a.coverage_level, a.number_incidents, a.number_accidents,
    a.number_duis, a.number_speedings, a.model_year, a.make, a.model, a.use, a.commute, a.annualmileage, a.ownership, a.residence_type, a.residence_occupancy, 
    a.property_policy_type, a.bi_pd_limit, a.umbi_limit, a.comp_coll_deductible, a.is_user_a_veteran) as o 
    on n.state = o.state and n.provider = o.provider and n.age = o.age and n.gender = o.gender and n.marital_status = o.marital_status and n.vehicles = o.vehicles and n.driving_record_violations = o.driving_record_violations and 
    n.insurancescore_alignment = o.insurancescore_alignment and n.coverage_level = o.coverage_level and n.number_incidents = o.number_incidents and n.number_accidents = o.number_accidents and n.number_duis = o.number_duis and 
    n.number_speedings = o.number_speedings and n.model_year = o.model_year and n.make = o.make and  n.model = o.model and  n.use = o.use and  n.commute = o.commute and  n.annualmileage = o.annualmileage and 
    n.ownership = o.ownership and n.residence_type = o.residence_type and n.residence_occupancy = o.residence_occupancy and n.property_policy_type = o.property_policy_type 
    and n.bi_pd_limit = o.bi_pd_limit and  n.umbi_limit = o.umbi_limit and n.comp_coll_deductible = o.comp_coll_deductible and  n.is_user_a_veteran = o.is_user_a_veteran 

                