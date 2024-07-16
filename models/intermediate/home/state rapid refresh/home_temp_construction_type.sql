--drop table if exists home_temp_construction_type;
--create table home_temp_construction_type as 

{{ config(materialized='table') }}
	WITH "Frame" AS (
	  SELECT state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		place_tier,place_tier_rank,claims_history,
		protection_class,construction_type,roof_type,all_perils_deductible,
		avg(annualpremium) as annualpremium
	  FROM ins_home_report_master_table_2023 
		where report = 'report_106' 
		and construction_type = 'Frame'
		and place_tier_rank = '4'
		group by state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		claims_history,all_perils_deductible,
		protection_class,construction_type,roof_type,
		place_tier,place_tier_rank
	),"Superior" AS(
		SELECT state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		place_tier,place_tier_rank,claims_history,
		protection_class,construction_type,roof_type,all_perils_deductible,
		avg(annualpremium) as annualpremium
	  FROM ins_home_report_master_table_2023 
		where report = 'report_106' 
		and construction_type = 'Superior'
		and place_tier_rank = '4'
		group by state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		claims_history,all_perils_deductible,
		protection_class,construction_type,roof_type,
		place_tier,place_tier_rank
	),"Masonry" AS (
	  SELECT state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		place_tier,place_tier_rank,claims_history,
		protection_class,construction_type,roof_type,all_perils_deductible,
		avg(annualpremium) as annualpremium
	  FROM ins_home_report_master_table_2023 
		where report = 'report_106' 
		and construction_type = 'Masonry'
		and place_tier_rank = '4'
		group by state,credit_tier,coverages,
		construction_year,cov_a_dwelling,provider,
		claims_history,all_perils_deductible,
		protection_class,construction_type,roof_type,
		place_tier,place_tier_rank
	),two AS (
	  SELECT *
		FROM {{ ref('home_temp_protection_class') }} 

	)--
	,scored AS (
		SELECT
		b.state,b.credit_tier,b.coverages,b.construction_year,
		b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
		b.claims_history,b.protection_class,b.construction_type,
		b.roof_type,b.all_perils_deductible,b.annualpremium as "Frame",
		a.annualpremium as "Superior",c.annualpremium as "Masonry",
		(a.annualpremium -b.annualpremium)/ b.annualpremium as score_sup,
		(c.annualpremium -b.annualpremium)/ b.annualpremium as score_man
		FROM "Frame" b  
		LEFT JOIN "Superior" a ON
		a.state = b.state AND
		a.coverages = b.coverages AND
		a.cov_a_dwelling = b.cov_a_dwelling AND
		a.construction_year = b.construction_year AND
		a.provider = b.provider AND
		a.claims_history = b.claims_history AND
		a.protection_class = b.protection_class AND
		a.roof_type = b.roof_type AND
		a.place_tier = b.place_tier AND
		a.place_tier_rank = b.place_tier_rank
		LEFT JOIN "Masonry" c ON
		c.state = b.state AND
		c.coverages = b.coverages AND
		c.cov_a_dwelling = b.cov_a_dwelling AND
		c.construction_year = b.construction_year AND
		c.provider = b.provider AND
		c.claims_history = b.claims_history AND
		c.protection_class = b.protection_class AND
		c.roof_type = b.roof_type AND
		c.place_tier = b.place_tier AND
		c.place_tier_rank = b.place_tier_rank
	)--
	,two_superior as (
		SELECT
		b.state,b.credit_tier,b.coverages,b.construction_year,
		b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
		b.claims_history,b.protection_class,'Superior' as construction_type,
		b.roof_type,b.all_perils_deductible,b.annualpremium as annual_102,
		a.score_sup,b.annualpremium + (b.annualpremium*a.score_sup) as annualpremium
		from two b
		LEFT JOIN scored a ON
		a.state = b.state AND
		a.provider = b.provider AND
		a.claims_history = b.claims_history AND
		a.roof_type = b.roof_type 
	)--
	,two_masonry as (
		SELECT
		b.state,b.credit_tier,b.coverages,b.construction_year,
		b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
		b.claims_history,b.protection_class,'Masonry' as construction_type,
		b.roof_type,b.all_perils_deductible,b.annualpremium as annual_102,
		a.score_man,b.annualpremium + (b.annualpremium*a.score_man) as annualpremium
		from two b
		LEFT JOIN scored a ON
		a.state = b.state AND
		a.provider = b.provider AND
		a.claims_history = b.claims_history AND
		a.roof_type = b.roof_type 
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
		from two_superior
	union all 
	select  state,
		credit_tier,coverages,construction_year,
		cov_a_dwelling,provider,place_tier,place_tier_rank,
		claims_history,protection_class,construction_type,
		roof_type,all_perils_deductible,annualpremium
		from two_masonry
		