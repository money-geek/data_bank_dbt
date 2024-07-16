--drop table if exists ins_home_master_table_extrapolated_data_state_insert_2024; --7685346
--create table ins_home_master_table_extrapolated_data_state_insert_2024 as

{{ config(materialized='table') }}

select a.*,n.state_provider_presence,b.Claims_Customer_Satisfaction,b.financial_stability,
	case when b.coverage is null then 0 else b.coverage end as coverage,m.provider_id,
	case when a.provider = 'USAA' then 'Yes' else 'No' end as is_user_veteran
from {{ ref('ins_home_master_table_extrapolated_data_state_2024_temp') }} a
LEFT JOIN ( SELECT national_provider,
            count(DISTINCT state)  AS state_provider_presence
			FROM {{ ref('ins_home_master_table_extrapolated_data_state_2024_temp') }}
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

