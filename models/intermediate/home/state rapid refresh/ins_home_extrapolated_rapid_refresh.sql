--drop table if exists ins_home_extrapolated_rapid_refresh;
--create table ins_home_extrapolated_rapid_refresh as

{{ config(materialized='table') }}
	select state,
		credit_tier,coverages,construction_year,
		cov_a_dwelling,provider,
		claims_history,protection_class,construction_type,
		roof_type,all_perils_deductible,
		avg(annualpremium) as annualpremium
	from {{ ref('home_master_table_extrapolated_alldata') }} 
	group by state,
		credit_tier,coverages,construction_year,
		cov_a_dwelling,provider,
		claims_history,protection_class,construction_type,
		roof_type,all_perils_deductible