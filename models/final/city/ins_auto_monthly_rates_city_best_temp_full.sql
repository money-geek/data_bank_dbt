{{ config(materialized='table') }}

select data_date as month,
	   state,
	   city,
	   provider,
	   age,
	   coverage,
	   avg(annual_premium) as annual_premium
from {{ ref('ins_auto_bestcheap_city_baseline_temp_inserted')}}
where vehicles = '2012 Toyota Camry LE'
and insurancescore_alignment =  'Good'
and driving_record_violations = 'Clean'
group by data_date, state, city,provider,age,coverage


--select * from dbt.ins_auto_monthly_rates_city_best_temp_full where city ='Akron' and age= '40' and coverage = 'Full Coverage'
--
--select distinct data_date from dbt.ins_auto_bestcheap_city_baseline_temp_inserted order by data_date