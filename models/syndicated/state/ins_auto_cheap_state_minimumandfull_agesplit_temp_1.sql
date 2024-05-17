

{{ config(materialized='table') }}
 
with minimun_coverage as(

    SELECT a.data_date,
            a.state,
            a.age,
            a.provider,
            a.coverage,
           a.state_pop_wt_rate_mandatory + a.state_pop_wt_rate_fees + a.state_pop_wt_rate_bi_pd + a.state_pop_wt_rate_umbi AS avg_annual_premium,
           (a.state_pop_wt_rate_mandatory + a.state_pop_wt_rate_fees + a.state_pop_wt_rate_bi_pd + a.state_pop_wt_rate_umbi)/12 AS avg_monthly_premium
         FROM ( SELECT 
                    data_date,
                    a.state,
                    a.age,
                    a.provider,
                    'Minimum Coverage' AS coverage,
                    avg(a.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
                    avg(a.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd,
                    avg(a.state_pop_wt_rate_comp_coll) AS state_pop_wt_rate_comp_coll,
                    avg(
                        CASE
                            WHEN a.state NOT IN ('Connecticut','Illinois', 'Kansas', 'Maine', 
                                    'Maryland', 'Massachusetts', 'Minnesota', 'Missouri', 
                                    'Nebraska', 'New Hampshire', 'New York', 'North Carolina', 
                                    'North Dakota', 'Oregon', 'South Carolina', 'South Dakota', 
                                    'Vermont', 'Virginia', 'District of Columbia', 'West Virginia', 'Wisconsin')
                            THEN 0::numeric                            
                            ELSE a.state_pop_wt_rate_umbi
                        END) AS state_pop_wt_rate_umbi,
                    avg(a.state_pop_wt_rate_fees) AS state_pop_wt_rate_fees
                   FROM {{ ref('ins_auto_pop_wt_new_premiums') }} a
                  WHERE  a.age in ('25','40','65')
                     AND a.vehicles = '2012 Toyota Camry LE'
                     AND a.insurancescore_alignment in ('Blank', 'Good')
                     AND a.driving_record_violations = 'Clean'
                     AND a.is_user_a_veteran = 'No' 
                     AND a.bi_pd_limit = 'State Minimum'
                  GROUP BY data_date, a.state, a.age, a.provider, 'Minimum Coverage'::text) a

), full_coverage as(
    SELECT b.data_date,
            b.state,
            b.age,
            b.provider,
            b.coverage,
            b.state_pop_wt_rate_mandatory + b.state_pop_wt_rate_fees + b.state_pop_wt_rate_bi_pd + b.state_pop_wt_rate_umbi + b.state_pop_wt_rate_comp_coll AS avg_annual_premium,
            (b.state_pop_wt_rate_mandatory + b.state_pop_wt_rate_fees + b.state_pop_wt_rate_bi_pd + b.state_pop_wt_rate_umbi + b.state_pop_wt_rate_comp_coll)/12 AS avg_monthly_premium

         FROM ( SELECT  data_date,
                    a.state,
                    a.age,
                    a.provider,
                    'Full Coverage' AS coverage,
                    avg(a.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
                    avg(a.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd,
                    avg(a.state_pop_wt_rate_comp_coll) AS state_pop_wt_rate_comp_coll,
                    avg(
                        CASE
                            WHEN a.state NOT IN ('Connecticut','Illinois', 'Kansas', 'Maine', 
                                    'Maryland', 'Massachusetts', 'Minnesota', 'Missouri', 
                                    'Nebraska', 'New Hampshire', 'New York', 'North Carolina', 
                                    'North Dakota', 'Oregon', 'South Carolina', 'South Dakota', 
                                    'Vermont', 'Virginia', 'District of Columbia', 'West Virginia', 'Wisconsin')
                            THEN 0::numeric  
                            ELSE a.state_pop_wt_rate_umbi
                        END) AS state_pop_wt_rate_umbi,
                    avg(a.state_pop_wt_rate_fees) AS state_pop_wt_rate_fees
                   FROM {{ ref('ins_auto_pop_wt_new_premiums') }} a
                  WHERE  (a.age in ('25','40','65'))
                    AND a.vehicles = '2012 Toyota Camry LE'
                    AND a.insurancescore_alignment in ('Blank', 'Good')
                     AND a.driving_record_violations = 'Clean'
                     AND a.is_user_a_veteran = 'No' 
                     AND a.bi_pd_limit = '100/300/100000' AND a.comp_coll_deductible = '1000/1000'
                  GROUP BY data_date, a.state, a.age, a.provider, 'Full Coverage'::text) b
), combined_data as (
    select * from minimun_coverage 
    union 
    select * from full_coverage 
), source_data as (
    SELECT  
    data_date ,
    c.state,
    c.age,
    c.provider,
    c.coverage,
    c.avg_annual_premium,
    c.avg_monthly_premium,
	case when c.age ='25' then '22-29' 
        when c.age ='40' then '30-59'
        when c.age ='65' then '60 +'
        end as age_range,
    dense_rank() OVER (PARTITION BY c.data_date, c.state, c.age, c.coverage ORDER BY c.avg_annual_premium) AS cheapest_rank,
	id.provider_id
    FROM combined_data c
    LEFT JOIN (select provider,
                provider_id 
                from ins_company_collection  
                where sub_vertical = 'Auto Insurance') id
    ON LOWER(c.provider) = id.provider 
    where id.provider_id is not null
) 

select 
    t1.data_date,
    t1.state,
    t1.age,
    t1.provider,
    t1.coverage,
    t1.avg_annual_premium,
    t1.avg_monthly_premium,
    t1.age_range,
    t1.cheapest_rank,
    t1.provider_id
from source_data t1 




