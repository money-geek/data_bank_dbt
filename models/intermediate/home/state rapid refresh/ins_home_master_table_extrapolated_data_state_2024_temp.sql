--drop table if exists ins_home_master_table_extrapolated_data_state_2024_temp;
--create table ins_home_master_table_extrapolated_data_state_2024_temp as

{{ config(materialized='table') }}

with cte1 as (
	select d.ratedate,
		b.state as state_name,
		a.state,
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
		a.annualpremium		
from {{ ref('home_master_table_extrapolated_alldata_2') }} a
left join (select distinct abbrev,state from ins_auto_geo_mapping_pouplation_2022) b
on b.abbrev = a.state
left join (select distinct provider,national_provider from ins_home_company_mapping_2023) c
on a.provider = c.provider
cross join (select distinct ratedate from {{ ref('ins_home_ratio_table_108_temp') }} where ratedate is not null) d

)

select 
	a.*,
	n.state_provider_presence,
	b.Claims_Customer_Satisfaction,
	b.financial_stability,
	case when b.coverage is null then 0 else b.coverage end as coverage,m.provider_id,
	case when a.provider = 'USAA' then 'Yes' else 'No' end as is_user_veteran
from cte1 a
LEFT JOIN ( SELECT national_provider,
            count(DISTINCT state)  AS state_provider_presence
			FROM cte1
			GROUP BY national_provider) n 
			ON a.national_provider = n.national_provider
LEFT JOIN ( SELECT provider,
            provider_id
			FROM ins_company_collection
			where sub_vertical = 'Home Insurance') m 
			ON a.provider = m.provider
left join home_scores_23 b
on a.provider = b.company
where annualpremium is not null
and a.provider != 'UPC Insurance'