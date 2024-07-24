
{{ config(materialized='table') }}

select data_date as month ,
	   national_provider,
	   age,
	   case when coverage = '100/300/100,000 - 1000 comp_coll' then 'Full Coverage'
	   when coverage = 'State Min - No comp_coll' then 'Minimum Coverage'
	   end as coverage,
	   avg(annual_premium) as annual_premium
from {{ ref('ins_auto_national_all_coverage_baseline_non_veteran_temp_full') }} 
where coverage in ('100/300/100,000 - 1000 comp_coll','State Min - No comp_coll')
group by data_date, national_provider,age,coverage



--select * from dbt.ins_auto_monthly_rates_national_best_temp_full where national_provider = 'AAA' and age = '40' and coverage = 'Full Coverage'