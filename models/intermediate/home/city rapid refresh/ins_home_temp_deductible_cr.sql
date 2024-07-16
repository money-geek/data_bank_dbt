{{ config(materialized='table') }}
WITH "1000" AS (
  SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_103' 
	and all_perils_deductible = '1000'
	and place_tier_rank = '4'
	and coverages = '$100K Dwelling / $50K Personal Property / $100K Liability'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),"500" AS (
    SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_103' 
	and all_perils_deductible = '500'
	and place_tier_rank = '4'
	and coverages = '$100K Dwelling / $50K Personal Property / $100K Liability'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),"1500" AS (
  SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_103' 
	and all_perils_deductible = '1500'
	and place_tier_rank = '4'
	and coverages = '$100K Dwelling / $50K Personal Property / $100K Liability'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),"2000" AS (
  SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_103' 
	and all_perils_deductible = '2000'
	and place_tier_rank = '4'
	and coverages = '$100K Dwelling / $50K Personal Property / $100K Liability'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),two AS (
  SELECT *
	FROM {{ ref('ins_home_temp_credit_credit_tier_cr') }}
	where annualpremium is not null
)--
,scored AS (
  	SELECT
    b.state,b.credit_tier,b.coverages,b.construction_year,
	b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
	b.claims_history,b.protection_class,b.construction_type,
	b.roof_type,b.all_perils_deductible,
	b.annualpremium as "1000",a.annualpremium as "500",
	c.annualpremium as "1500",d.annualpremium as "2000",
	(a.annualpremium -b.annualpremium)/ b.annualpremium as score_500,
	(c.annualpremium -b.annualpremium)/ b.annualpremium as score_1500,
	(d.annualpremium -b.annualpremium)/ b.annualpremium as score_2000
    FROM "1000" b  
	LEFT JOIN "500" a ON
    a.state = b.state AND
    a.coverages = b.coverages AND
    a.cov_a_dwelling = b.cov_a_dwelling AND
    a.construction_year = b.construction_year AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.protection_class = b.protection_class AND
    a.construction_type = b.construction_type AND
    a.roof_type = b.roof_type AND
    a.place_tier = b.place_tier AND
    a.place_tier_rank = b.place_tier_rank
	LEFT JOIN "1500" c ON
    c.state = b.state AND
	c.coverages = b.coverages AND
    c.cov_a_dwelling = b.cov_a_dwelling AND
    c.construction_year = b.construction_year AND
    c.provider = b.provider AND
	c.claims_history = b.claims_history AND
    c.protection_class = b.protection_class AND
    c.construction_type = b.construction_type AND
    c.roof_type = b.roof_type AND
    c.place_tier = b.place_tier AND
    c.place_tier_rank = b.place_tier_rank
	LEFT JOIN "2000" d ON
    d.state = b.state AND
	d.coverages = b.coverages AND
    d.cov_a_dwelling = b.cov_a_dwelling AND
    d.construction_year = b.construction_year AND
    d.provider = b.provider AND
	d.claims_history = b.claims_history AND
    d.protection_class = b.protection_class AND
    d.construction_type = b.construction_type AND
    d.roof_type = b.roof_type AND
    d.place_tier = b.place_tier AND
    d.place_tier_rank = b.place_tier_rank
)--
,two_500 as (
	SELECT
    b.state,b.city,b.credit_tier,b.coverages,b.construction_year,
	b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
	b.claims_history,b.protection_class,b.construction_type,
	b.roof_type,500 as all_perils_deductible,b.annualpremium as annual_101,
	a.score_500,b.annualpremium + (b.annualpremium*a.score_500) as annualpremium
	from two b
	LEFT JOIN scored a ON
    a.state = b.state AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.protection_class = b.protection_class AND
    a.construction_type = b.construction_type AND
    a.roof_type = b.roof_type 
)--
,two_1500 as (
	SELECT
    b.state,b.city,b.credit_tier,b.coverages,b.construction_year,
	b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
	b.claims_history,b.protection_class,b.construction_type,
	b.roof_type,1500 as all_perils_deductible,b.annualpremium as annual_101,
	a.score_1500,b.annualpremium + (b.annualpremium*a.score_1500) as annualpremium
	from two b
	LEFT JOIN scored a ON
    a.state = b.state AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.protection_class = b.protection_class AND
    a.construction_type = b.construction_type AND
    a.roof_type = b.roof_type 
)--
,two_2000 as (
	SELECT
    b.state,b.city,b.credit_tier,b.coverages,b.construction_year,
	b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,
	b.claims_history,b.protection_class,b.construction_type,
	b.roof_type,2000 as all_perils_deductible,b.annualpremium as annual_101,
	a.score_2000,b.annualpremium + (b.annualpremium*a.score_2000) as annualpremium
	from two b
	LEFT JOIN scored a ON
    a.state = b.state AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.protection_class = b.protection_class AND
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
	from two_500
union all 
select  state,
	city,credit_tier,coverages,construction_year,
	cov_a_dwelling,provider,place_tier,place_tier_rank,
	claims_history,protection_class,construction_type,
	roof_type,all_perils_deductible,annualpremium
	from two_1500
union all 
select  state,
	city,credit_tier,coverages,construction_year,
	cov_a_dwelling,provider,place_tier,place_tier_rank,
	claims_history,protection_class,construction_type,
	roof_type,all_perils_deductible,annualpremium
	from two_2000