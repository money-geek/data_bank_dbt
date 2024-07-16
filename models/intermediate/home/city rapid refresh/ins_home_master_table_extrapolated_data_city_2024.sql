{{ config(materialized='table') }}

with cte_provider as (
		select distinct provider,national_provider from ins_home_company_mapping_2023 
	), state_name as (
		select distinct abbrev,state from ins_auto_geo_mapping_pouplation_2022
	), provider_id as  (
		SELECT provider,
	            provider_id
				FROM ins_company_collection
				where sub_vertical = 'Home Insurance'
	)	
	select b.state as state_name,
			a.state,
			a.city,
			a.provider,
			c.national_provider,
			a.credit_tier,
			a.coverages,
			a.construction_year::character varying,
			case when a.claims_history = '2 claims in past 5 year' then '2 claims in past 5 years'
				when a.claims_history = '1 claim in past 5 year' then '1 claim in past 5 years'
				else a.claims_history end as claims_history,
			a.protection_class::character varying,
			a.construction_type,
			a.roof_type,
			a.all_perils_deductible::character varying,
			a.annualpremium,
			d.ratedate,
			e.Claims_Customer_Satisfaction,
			e.financial_stability,
		    e.coverage,
			m.provider_id,
			case when a.provider = 'USAA' then 'Yes' else 'No' end as is_user_veteran
		from {{ ref('ins_home_master_table_extrapolated_alldata_cr') }} a
		left join state_name b on b.abbrev = a.state
		left join cte_provider c on a.provider = c.provider
		LEFT JOIN provider_id m ON a.provider = m.provider
		cross join (select max(ratedate) as ratedate from ins_home_ratio_table_108_temp) d
		left join home_scores_23 e on a.provider = e.company