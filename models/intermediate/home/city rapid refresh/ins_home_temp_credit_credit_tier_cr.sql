{{ config(materialized='table') }}


WITH "Good" AS (
  SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_102' 
	and place_tier_rank = '4'
	and credit_tier = 'Good'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),"Excellent" AS (
    SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_102'
	and place_tier_rank = '4'
	and credit_tier = 'Excellent'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),"Fair" AS (
  SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_102'
	and place_tier_rank = '4'
	and credit_tier = 'Fair'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),"Below_Fair" AS (
  SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_102'
	and place_tier_rank = '4'
	and credit_tier = 'Below Fair'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),"Poor" AS (
  SELECT state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM ins_home_report_master_table_2023 
	where report = 'report_102' 
	and place_tier_rank = '4'
	and credit_tier = 'Poor'
	group by state,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
),one AS (
  SELECT state,city,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	place_tier,place_tier_rank,claims_history,
	protection_class,construction_type,roof_type,all_perils_deductible,
	avg(annualpremium) as annualpremium
  FROM {{ ref('ins_home_ratio_table_101_temp') }} 
	where place_tier is not null
	group by state,city,credit_tier,coverages,
	construction_year,cov_a_dwelling,provider,
	claims_history,all_perils_deductible,
	protection_class,construction_type,roof_type,
	place_tier,place_tier_rank
)--
,scored AS (
  	SELECT
    b.state,
    b.credit_tier,
	b.coverages,
	b.construction_year,
	b.cov_a_dwelling,
    b.provider,
	b.place_tier,
	b.place_tier_rank,
	b.claims_history,
	b.protection_class,
	b.construction_type,
	b.roof_type,
	b.all_perils_deductible,
	b.annualpremium as good,
	a.annualpremium as excellent,
	c.annualpremium as fair,
	d.annualpremium as below,
	e.annualpremium as poor,
	(a.annualpremium -b.annualpremium)/ b.annualpremium as score_excellent,
	(c.annualpremium -b.annualpremium)/ b.annualpremium as score_fair,
	(d.annualpremium -b.annualpremium)/ b.annualpremium as score_bellow,
	(e.annualpremium -b.annualpremium)/ b.annualpremium as score_poor
    FROM "Good" b  
	LEFT JOIN "Excellent" a ON
    a.state = b.state AND
    a.coverages = b.coverages AND
    a.cov_a_dwelling = b.cov_a_dwelling AND
    a.construction_year = b.construction_year AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.protection_class = b.protection_class AND
    a.construction_type = b.construction_type AND
    a.roof_type = b.roof_type AND
    a.all_perils_deductible = b.all_perils_deductible AND
    a.place_tier = b.place_tier AND
    a.place_tier_rank = b.place_tier_rank
	LEFT JOIN "Fair" c ON
    c.state = b.state AND
	c.coverages = b.coverages AND
    c.cov_a_dwelling = b.cov_a_dwelling AND
    c.construction_year = b.construction_year AND
    c.provider = b.provider AND
	c.claims_history = b.claims_history AND
    c.protection_class = b.protection_class AND
    c.construction_type = b.construction_type AND
    c.roof_type = b.roof_type AND
	c.all_perils_deductible = b.all_perils_deductible AND
    c.place_tier = b.place_tier AND
    c.place_tier_rank = b.place_tier_rank
	LEFT JOIN "Below_Fair" d ON
    d.state = b.state AND
	d.coverages = b.coverages AND
    d.cov_a_dwelling = b.cov_a_dwelling AND
    d.construction_year = b.construction_year AND
    d.provider = b.provider AND
	d.claims_history = b.claims_history AND
    d.protection_class = b.protection_class AND
    d.construction_type = b.construction_type AND
    d.roof_type = b.roof_type AND
	d.all_perils_deductible = b.all_perils_deductible AND
    d.place_tier = b.place_tier AND
    d.place_tier_rank = b.place_tier_rank
	LEFT JOIN "Poor" e ON
    e.state = b.state AND
	e.coverages = b.coverages AND
    e.cov_a_dwelling = b.cov_a_dwelling AND
    e.construction_year = b.construction_year AND
    e.provider = b.provider AND
	e.claims_history = b.claims_history AND
    e.protection_class = b.protection_class AND
    e.construction_type = b.construction_type AND
    e.roof_type = b.roof_type AND
	e.all_perils_deductible = b.all_perils_deductible AND
    e.place_tier = b.place_tier AND
    e.place_tier_rank = b.place_tier_rank
)--
,one_exc as (
	SELECT
    b.state,b.city,'Excellent' as credit_tier,b.coverages,b.construction_year,
	b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,b.claims_history,
	b.protection_class,b.construction_type,b.roof_type,b.all_perils_deductible,
	b.annualpremium as annual_101,a.score_excellent,
	b.annualpremium + (b.annualpremium*a.score_excellent) as annualpremium
	from one b
	LEFT JOIN scored a ON
    a.state = b.state AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.protection_class = b.protection_class AND
    a.construction_type = b.construction_type AND
    a.roof_type = b.roof_type AND
	a.all_perils_deductible = b.all_perils_deductible
)--
,one_fair as (
	SELECT
    b.state,b.city,'Fair' as credit_tier,b.coverages,b.construction_year,
	b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,b.claims_history,
	b.protection_class,b.construction_type,b.roof_type,b.all_perils_deductible,
	b.annualpremium as annual_101,a.score_fair,
	b.annualpremium + (b.annualpremium*a.score_fair) as annualpremium
	from one b
	LEFT JOIN scored a ON
    a.state = b.state AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.protection_class = b.protection_class AND
    a.construction_type = b.construction_type AND
    a.roof_type = b.roof_type AND
	a.all_perils_deductible = b.all_perils_deductible
)--
,one_bellow as (
	SELECT
    b.state,b.city,'Below Fair' as credit_tier,b.coverages,b.construction_year,
	b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,b.claims_history,
	b.protection_class,b.construction_type,b.roof_type,b.all_perils_deductible,
	b.annualpremium as annual_101,a.score_bellow,
	b.annualpremium + (b.annualpremium*a.score_bellow) as annualpremium
	from one b
	LEFT JOIN scored a ON
    a.state = b.state AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.protection_class = b.protection_class AND
    a.construction_type = b.construction_type AND
    a.roof_type = b.roof_type AND
	a.all_perils_deductible = b.all_perils_deductible
)--
,one_poor as (
	SELECT
    b.state,b.city,'Poor' as credit_tier,b.coverages,b.construction_year,
	b.cov_a_dwelling,b.provider,b.place_tier,b.place_tier_rank,b.claims_history,
	b.protection_class,b.construction_type,b.roof_type,b.all_perils_deductible,
	b.annualpremium as annual_101,a.score_poor,
	b.annualpremium + (b.annualpremium*a.score_poor) as annualpremium
	from one b
	LEFT JOIN scored a ON
    a.state = b.state AND
    a.provider = b.provider AND
	a.claims_history = b.claims_history AND
    a.protection_class = b.protection_class AND
    a.construction_type = b.construction_type AND
    a.roof_type = b.roof_type AND
	a.all_perils_deductible = b.all_perils_deductible 
) select  state,
	city,credit_tier,coverages,construction_year,
	cov_a_dwelling,provider,place_tier,place_tier_rank,
	claims_history,protection_class,construction_type,
	roof_type,all_perils_deductible,annualpremium
	from one
union all 
select  state,
	city,credit_tier,coverages,construction_year,
	cov_a_dwelling,provider,place_tier,place_tier_rank,
	claims_history,protection_class,construction_type,
	roof_type,all_perils_deductible,annualpremium
	from one_exc
union all 
select  state,
	city,credit_tier,coverages,construction_year,
	cov_a_dwelling,provider,place_tier,place_tier_rank,
	claims_history,protection_class,construction_type,
	roof_type,all_perils_deductible,annualpremium
	from one_fair
union all 
select  state,
	city,credit_tier,coverages,construction_year,
	cov_a_dwelling,provider,place_tier,place_tier_rank,
	claims_history,protection_class,construction_type,
	roof_type,all_perils_deductible,annualpremium
	from one_bellow
union all 
select  state,
	city,credit_tier,coverages,construction_year,
	cov_a_dwelling,provider,place_tier,place_tier_rank,
	claims_history,protection_class,construction_type,
	roof_type,all_perils_deductible,annualpremium
	from one_poor