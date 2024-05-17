 {{ config(materialized='table') }}
 with t1 as (
    SELECT 
    a.data_date,
    a.state,
    a.state_code,
    a.national_provider,
    a.provider,
    b.national_presence,
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
	case 
        when a.insurancescore_alignment = 'Blank' then 'Good'
	    else a.insurancescore_alignment end as insurancescore_alignment,
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
 --   a.quadrant_report,
    sum(coalesce(a.wt_rate,0)) AS state_annualpremium,
    sum(coalesce(a.wt_rate_mandatory,0)) AS state_pop_wt_rate_mandatory,
    sum(coalesce(a.wt_rate_bi_pd,0)) AS state_pop_wt_rate_bi_pd,
    sum(coalesce(a.wt_rate_comp_coll,0)) AS state_pop_wt_rate_comp_coll,
    sum(coalesce(a.wt_rate_umbi,0)) AS state_pop_wt_rate_umbi,
    sum(coalesce(a.wt_rate_fees,0)) AS state_pop_wt_rate_fees
    FROM {{ ref('ins_auto_monthly_new_premiums') }} a
    LEFT JOIN {{ ref('ins_auto_monthly_national_presence') }} b on a.national_provider = b.national_provider 
    GROUP BY a.data_date,a.state,a.national_provider,b.national_presence ,a.state_code, a.provider, a.age, a.gender, a.marital_status, a.drv2_age, a.drv2_gender, a.drv2_marital_status, a.drv3_age, a.drv3_gender, 
    a.drv3_marital_status, a.vehicles, a.driving_record_violations, a.insurancescore_alignment, a.coverage_level, a.number_incidents, a.number_accidents,
    a.number_duis, a.number_speedings, a.model_year, a.make, a.model, a.use, a.commute, a.annualmileage, a.ownership, a.residence_type, a.residence_occupancy, 
    a.property_policy_type, a.bi_pd_limit, a.umbi_limit, a.comp_coll_deductible, a.is_user_a_veteran
),t2 
as (
    select  
    state_name as state ,national_provider,
    avg(stability_score) as stability_score,
    avg(claims_score) as claims_score,
    avg(satisfaction_score)  as satisfaction_score,
    avg(coverage_score) as coverage_score
from ins_auto_scores_2023
group by 1,2 )	
        

select t1.*,t2.stability_score::int,
        t2.claims_score::int,t2.coverage_score::int
        ,t2.satisfaction_score::int 
from t1 
left join t2 
on t1.national_provider = t2.national_provider 
and t1.state = t2.state
where t1.state_annualpremium != 0