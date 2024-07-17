
{{ config(materialized='table') }}

select FORMAT(data_date, 'MM/dd/yyyy') as month,
   state,
   provider,
   age,
   coverage,
   avg(annual_premium) as annual_premium
from {{ ref('ins_auto_best_state_baseline_temp_full') }}
group by FORMAT(data_date, 'MM/dd/yyyy'), state, provider,age,coverage

union 	

select FORMAT(data_date, 'MM/dd/yyyy') as month,
	state,
   provider,
   age,
   coverage,
   avg(annual_premium) as annual_premium
from {{ ref('ins_auto_best_state_minimum_temp_full') }}
group by FORMAT(data_date, 'MM/dd/yyyy'), state, provider,age,coverage
order by state



--select * from dbt.ins_auto_monthly_rates_state_best_temp_full where state = 'Alabama' and month='02/01/2024' and age ='40' and coverage = 'Full Coverage'