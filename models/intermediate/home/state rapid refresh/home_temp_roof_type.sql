--drop table if exists home_temp_roof_type;
--create table home_temp_roof_type as 

{{ config(materialized='table') }}
	WITH "Composition" AS (
	  SELECT state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		place_tier,place_tier_rank,claims_history,
		protection_class,construction_type,roof_type,all_perils_deductible,
		avg(annualpremium) as annualpremium
	  FROM ins_home_report_master_table_2023
		where report = 'report_107' 
		and roof_type = 'Composition'
		and place_tier_rank = '4'
		group by state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		claims_history,all_perils_deductible,
		protection_class,construction_type,roof_type,
		place_tier,place_tier_rank
	),"Shake-Treated" AS(
		SELECT state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		place_tier,place_tier_rank,claims_history,
		protection_class,construction_type,roof_type,all_perils_deductible,
		avg(annualpremium) as annualpremium
	  FROM ins_home_report_master_table_2023
		where report = 'report_107' 
		and roof_type = 'Shake-Treated'
		and place_tier_rank = '4'
		group by state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		claims_history,all_perils_deductible,
		protection_class,construction_type,roof_type,
		place_tier,place_tier_rank
	),"Tile" AS (
	  SELECT state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		place_tier,place_tier_rank,claims_history,
		protection_class,construction_type,roof_type,all_perils_deductible,
		avg(annualpremium) as annualpremium
	  FROM ins_home_report_master_table_2023 
		where report = 'report_107' 
		and roof_type = 'Tile'
		and place_tier_rank = '4'
		group by state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		claims_history,all_perils_deductible,
		protection_class,construction_type,roof_type,
		place_tier,place_tier_rank
	),two AS (
	  SELECT *
	  FROM {{ ref('home_temp_construction_type') }} 
	)--
	,scored AS (
		SELECT
		b.state,b.credit_tier,b.coverages,b.construction_year,
		b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
		b.claims_history,b.protection_class,b.construction_type,
		b.roof_type,b.all_perils_deductible,b.annualpremium as "composit",
		a.annualpremium as "shaketreated",c.annualpremium as "til",
		(a.annualpremium -b.annualpremium)/ b.annualpremium as score_shake,
		(c.annualpremium -b.annualpremium)/ b.annualpremium as score_tile
		FROM "Composition" b  
		LEFT JOIN "Shake-Treated" a ON
		a.state = b.state AND
		a.coverages = b.coverages AND
		a.cov_a_dwelling = b.cov_a_dwelling AND
		a.construction_year = b.construction_year AND
		a.provider = b.provider AND
		a.claims_history = b.claims_history AND
		a.protection_class = b.protection_class AND
		a.construction_type = b.construction_type AND
		a.place_tier = b.place_tier AND
		a.place_tier_rank = b.place_tier_rank
		LEFT JOIN "Tile" c ON
		c.state = b.state AND
		c.coverages = b.coverages AND
		c.cov_a_dwelling = b.cov_a_dwelling AND
		c.construction_year = b.construction_year AND
		c.provider = b.provider AND
		c.claims_history = b.claims_history AND
		c.protection_class = b.protection_class AND
		c.construction_type = b.construction_type AND
		c.place_tier = b.place_tier AND
		c.place_tier_rank = b.place_tier_rank
	)--
	,two_shake as (
		SELECT
		b.state,b.credit_tier,b.coverages,b.construction_year,
		b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
		b.claims_history,b.protection_class,b.construction_type,
		'Shake-Treated' as roof_type,b.all_perils_deductible,b.annualpremium as annual_102,
		a.score_shake,b.annualpremium + (b.annualpremium*a.score_shake) as annualpremium
		from two b
		LEFT JOIN scored a ON
		a.state = b.state AND
		a.provider = b.provider AND
		a.claims_history = b.claims_history 
	)--
	,two_Tile as (
		SELECT
		b.state,b.credit_tier,b.coverages,b.construction_year,
		b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
		b.claims_history,b.protection_class,b.construction_type,
		'Tile' as roof_type,b.all_perils_deductible,b.annualpremium as annual_102,
		a.score_tile,b.annualpremium + (b.annualpremium*a.score_tile) as annualpremium
		from two b
		LEFT JOIN scored a ON
		a.state = b.state AND
		a.provider = b.provider AND
		a.claims_history = b.claims_history  
	)
	 select  state,
		credit_tier,coverages,construction_year,
		cov_a_dwelling,provider,place_tier,place_tier_rank,
		claims_history,protection_class,construction_type,
		roof_type,all_perils_deductible,annualpremium
		from two
	union all 
	select  state,
		credit_tier,coverages,construction_year,
		cov_a_dwelling,provider,place_tier,place_tier_rank,
		claims_history,protection_class,construction_type,
		roof_type,all_perils_deductible,annualpremium
		from two_shake
	union all 
	select  state,
		credit_tier,coverages,construction_year,
		cov_a_dwelling,provider,place_tier,place_tier_rank,
		claims_history,protection_class,construction_type,
		roof_type,all_perils_deductible,annualpremium
		from two_Tile
