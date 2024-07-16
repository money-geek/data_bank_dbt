{{ config(materialized='table') }}
WITH "3" AS (
  SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_105' 
	and protection_class = '3'
	and place_tier_rank = '4'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),"5" AS(
    SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_105' 
	and protection_class = '5'
	and place_tier_rank = '4'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),"7" AS (
  SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_105' 
	and protection_class = '7'
	and place_tier_rank = '4'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),"8" AS(
  SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_105' 
	and protection_class = '8'
	and place_tier_rank = '4'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),two AS (
  SELECT *
	FROM {{ ref('ins_home_temp_deductible_cr') }} 

)--
,scored AS (
  	SELECT
    b.state,b.credit_tier,b.coverages,b.construction_year,
	b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
	b.claims_history,b.protection_class,b.construction_type,
	b.roof_type,b.all_perils_deductible,b.annualpremium as "3",
	a.annualpremium as "5",c.annualpremium as "7",d.annualpremium as "8",
	(a.annualpremium -b.annualpremium)/ b.annualpremium as score_5,
	(c.annualpremium -b.annualpremium)/ b.annualpremium as score_7,
	(d.annualpremium -b.annualpremium)/ b.annualpremium as score_8
    FROM "3" b  
	LEFT JOIN "5" a ON
    a.state = b.state AND
    a.coverages = b.coverages AND
    a.cov_a_dwelling = b.cov_a_dwelling AND
    a.construction_year = b.construction_year AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.construction_type = b.construction_type AND
    a.roof_type = b.roof_type AND
    a.place_tier = b.place_tier AND
    a.place_tier_rank = b.place_tier_rank
	LEFT JOIN "7" c ON
    c.state = b.state AND
	c.coverages = b.coverages AND
    c.cov_a_dwelling = b.cov_a_dwelling AND
    c.construction_year = b.construction_year AND
    c.provider = b.provider AND
	c.claims_history = b.claims_history AND
    c.construction_type = b.construction_type AND
    c.roof_type = b.roof_type AND
    c.place_tier = b.place_tier AND
    c.place_tier_rank = b.place_tier_rank
	LEFT JOIN "8" d ON
    d.state = b.state AND
	d.coverages = b.coverages AND
    d.cov_a_dwelling = b.cov_a_dwelling AND
    d.construction_year = b.construction_year AND
    d.provider = b.provider AND
	d.claims_history = b.claims_history AND
    d.construction_type = b.construction_type AND
    d.roof_type = b.roof_type AND
    d.place_tier = b.place_tier AND
    d.place_tier_rank = b.place_tier_rank
)--
,two_5 as (
	SELECT
    b.state,b.city,b.credit_tier,b.coverages,b.construction_year,
	b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
	b.claims_history,5 AS protection_class,b.construction_type,
	b.roof_type,b.all_perils_deductible,b.annualpremium as annual_102,
	a.score_5,b.annualpremium + (b.annualpremium*a.score_5) as annualpremium
	from two b
	LEFT JOIN scored a ON
    a.state = b.state AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.construction_type = b.construction_type AND
    a.roof_type = b.roof_type 
)--
,two_7 as (
	SELECT
    b.state,b.city,b.credit_tier,b.coverages,b.construction_year,
	b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
	b.claims_history,7 AS protection_class,b.construction_type,
	b.roof_type,b.all_perils_deductible,b.annualpremium as annual_102,
	a.score_7,b.annualpremium + (b.annualpremium*a.score_7) as annualpremium
	from two b
	LEFT JOIN scored a ON
    a.state = b.state AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.construction_type = b.construction_type AND
    a.roof_type = b.roof_type 
)--
,two_8 as (
		SELECT
    b.state,b.city,b.credit_tier,b.coverages,b.construction_year,
	b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
	b.claims_history,8 AS protection_class,b.construction_type,
	b.roof_type,b.all_perils_deductible,b.annualpremium as annual_102,
	a.score_8,b.annualpremium + (b.annualpremium*a.score_8) as annualpremium
	from two b
	LEFT JOIN scored a ON
    a.state = b.state AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.construction_type = b.construction_type AND
    a.roof_type = b.roof_type
)
 select  state,
 	city,credit_tier,coverages,construction_year,
	cov_a_dwelling,provider,place_tier,place_tier_rank,
	claims_history,protection_class,construction_type,
	roof_type,all_perils_deductible,annualpremium
	from two
union all 
select  state,
	city,credit_tier,coverages,construction_year,
	cov_a_dwelling,provider,place_tier,place_tier_rank,
	claims_history,protection_class,construction_type,
	roof_type,all_perils_deductible,annualpremium
	from two_5
union all 
select  state,
	city,credit_tier,coverages,construction_year,
	cov_a_dwelling,provider,place_tier,place_tier_rank,
	claims_history,protection_class,construction_type,
	roof_type,all_perils_deductible,annualpremium
	from two_7
union all 
select  state,
	city,credit_tier,coverages,construction_year,
	cov_a_dwelling,provider,place_tier,place_tier_rank,
	claims_history,protection_class,construction_type,
	roof_type,all_perils_deductible,annualpremium
	from two_8