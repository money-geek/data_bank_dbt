--01/01/2024 Eddy Creating the ratio Table ins_home_ratio_table_108_temp
{{ config(materialized='table') }}
with recurring_city  as
	(
		SELECT ratedate,provider, state,city,place_tier_rank,AVG(annualpremium) AS annualpremium_avg 
			 FROM {{ ref('ins_home_report_master_table_108_temp') }}
			 --WHERE TO_DATE(ratedate,'MM-DD-YYYY') = (select MAX(data_date) FROM ins_home_data_date)
			 WHERE TO_DATE(ratedate,'MM-DD-YYYY') = '{{ var("current_month") }}'
			 --and place_tier_rank = '2' and state = 'FL'-- and provider in ('Nationwide')
			 GROUP BY ratedate,provider, state,city,place_tier_rank
	), old_city as 
		(
			 SELECT provider, state,city, place_tier_rank, AVG(annualpremium) AS annualpremium_avg 
			 FROM ins_home_report_master_table_2023 
			 WHERE report = 'report_101' and place_tier_rank is not null 
			 and coverages = '$100K Dwelling / $50K Personal Property / $100K Liability'
			 and construction_year = 2000
			 GROUP BY provider, state,city,place_tier_rank 
	),rate_city as(
		select  a.ratedate::date,
			a.provider,
			a.state,
			a.city,
			a.place_tier_rank,
			b.annualpremium_avg AS Avg_AnnualPremium_2023,
			a.annualpremium_avg AS Avg_AnnualPremium_108,
			(a.annualpremium_avg / b.annualpremium_avg)  AS rate	
			from recurring_city a
			join old_city b
			ON b.provider = a.provider 
				 AND b.state = a.state 
				 and b.place_tier_rank = a.place_tier_rank 
				 and b.city = a.city
		)
	select b.ratedate,
			a.provider,
			--population,	
			a.state,
			a.city,
			a.place_tier_rank,
			a.annualpremium_avg as pre_annualpremium,
			b.rate,
			a.annualpremium_avg * rate as annualpremium
	from 
		(SELECT * from old_city) a
	left join rate_city b
	  ON b.provider = a.provider 
	 AND b.state = a.state
	 and b.place_tier_rank = a.place_tier_rank 