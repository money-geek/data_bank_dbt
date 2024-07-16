--drop table if exists home_master_table_extrapolated_alldata;
--create table home_master_table_extrapolated_alldata as 

{{ config(materialized='table') }}
	WITH "Claim5" AS (
	  SELECT state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		place_tier,place_tier_rank,claims_history,
		protection_class,construction_type,roof_type,all_perils_deductible,
		avg(annualpremium) as annualpremium
	  FROM ins_home_report_master_table_2023 
		where report = 'report_104' 
		and claims_history = 'Claim free for 5+ years'
		and place_tier_rank = '4'
		group by state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		claims_history,all_perils_deductible,
		protection_class,construction_type,roof_type,
		place_tier,place_tier_rank
	),"claim1" AS(
		SELECT state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		place_tier,place_tier_rank,claims_history,
		protection_class,construction_type,roof_type,all_perils_deductible,
		avg(annualpremium) as annualpremium
	  FROM ins_home_report_master_table_2023 
		where report = 'report_104' 
		and claims_history = '1 claim in past 5 year'
		and place_tier_rank = '4'
		group by state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		claims_history,all_perils_deductible,
		protection_class,construction_type,roof_type,
		place_tier,place_tier_rank
	),"claim2" AS (
	  SELECT state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		place_tier,place_tier_rank,claims_history,
		protection_class,construction_type,roof_type,all_perils_deductible,
		avg(annualpremium) as annualpremium
	  FROM ins_home_report_master_table_2023 
		where report = 'report_104' 
		and claims_history = '2 claims in past 5 year'
		and place_tier_rank = '4'
		group by state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		claims_history,all_perils_deductible,
		protection_class,construction_type,roof_type,
		place_tier,place_tier_rank
	),two AS (
	  SELECT *
		FROM {{ ref('home_temp_roof_type') }}
		where annualpremium is not null
	)--
	,scored AS (
		SELECT
		b.state,b.credit_tier,b.coverages,b.construction_year,
		b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
		b.claims_history,b.protection_class,b.construction_type,
		b.roof_type,b.all_perils_deductible,b.annualpremium as "claim5",
		a.annualpremium as "claim1",c.annualpremium as "claim2",
		(a.annualpremium -b.annualpremium)/ b.annualpremium as score_1,
		(c.annualpremium -b.annualpremium)/ b.annualpremium as score_2
		FROM "Claim5" b  
		LEFT JOIN "claim1" a ON
		a.state = b.state AND
		a.coverages = b.coverages AND
		a.cov_a_dwelling = b.cov_a_dwelling AND
		a.construction_year = b.construction_year AND
		a.provider = b.provider AND
		a.protection_class = b.protection_class AND
		a.construction_type = b.construction_type AND
		a.roof_type = b.roof_type AND
		a.place_tier = b.place_tier AND
		a.place_tier_rank = b.place_tier_rank
		LEFT JOIN "claim2" c ON
		c.state = b.state AND
		c.coverages = b.coverages AND
		c.cov_a_dwelling = b.cov_a_dwelling AND
		c.construction_year = b.construction_year AND
		c.provider = b.provider AND
		c.protection_class = b.protection_class AND
		c.construction_type = b.construction_type AND
		c.roof_type = b.roof_type AND
		c.place_tier = b.place_tier AND
		c.place_tier_rank = b.place_tier_rank
	)--
	,two_claim1 as (
		SELECT
		b.state,b.credit_tier,b.coverages,b.construction_year,
		b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
		'1 claim in past 5 year' as claims_history,b.protection_class,
		b.construction_type,b.roof_type,b.all_perils_deductible,
		b.annualpremium as annual_102,a.score_1,
		b.annualpremium + (b.annualpremium*a.score_1) as annualpremium
		from two b
		LEFT JOIN scored a ON
		a.state = b.state AND
		a.provider = b.provider 
	),two_claim2 as (
		SELECT
		b.state,b.credit_tier,b.coverages,b.construction_year,
		b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
		'2 claims in past 5 year' as claims_history,b.protection_class,
		b.construction_type,b.roof_type,b.all_perils_deductible,
		b.annualpremium as annual_102,a.score_2,
		b.annualpremium + (b.annualpremium*a.score_2) as annualpremium
		from two b
		LEFT JOIN scored a ON
		a.state = b.state AND
		a.provider = b.provider 
	)
	 select  state,
		credit_tier,coverages,construction_year,
		cov_a_dwelling,provider,place_tier,place_tier_rank,
		claims_history,protection_class,construction_type,
		roof_type,all_perils_deductible,annualpremium
		from two
	union
	select  state,
		credit_tier,coverages,construction_year,
		cov_a_dwelling,provider,place_tier,place_tier_rank,
		claims_history,protection_class,construction_type,
		roof_type,all_perils_deductible,annualpremium
		from two_claim1
	union
	select  
		state,
		credit_tier,coverages,construction_year,
		cov_a_dwelling,provider,place_tier,place_tier_rank,
		claims_history,protection_class,construction_type,
		roof_type,all_perils_deductible,annualpremium
		from two_claim2
