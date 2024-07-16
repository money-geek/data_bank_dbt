{{ config(materialized='table') }}

	SELECT combined.*, 
			m.provider,
			m.national_provider,
			population,
			abbrev,
			display_name as city, 
			m1.state as state_name,
			place_tier,
			place_tier_rank
		FROM ins_home_company_mapping_2023 m
		LEFT JOIN
		(
			SELECT 
			'report_108' as report,	 	
			TO_CHAR(TO_DATE(a.ratedate, 'YYYYMM'), 'MM/DD/YYYY')as ratedate,
			a.state,
			a.riskid,
			CASE WHEN LENGTH(a.zipcode) = 4 THEN CONCAT('0',a.zipcode)ELSE a.zipcode
			END AS zipcode,
			CASE WHEN a.credit_tier is null THEN 'Good'ELSE a.credit_tier
			END AS credit_tier,
			a.coverages,
			a.construction_year::integer,
			CASE WHEN a.claims_history = '0' THEN 'Claim free for 5+ years'ELSE a.claims_history
			end as claims_history,
			a.protection_class::integer,
			a.construction_type,
			a.roof_type,
			a.cov_a_dwelling::double precision,
			a.cov_b_other_structures::double precision,
			a.cov_c_personal_property::double precision,
			a.cov_d_loss_of_use::double precision,
			a.liabilty_limit::double precision,
			a.med_limit::double precision,
			a.all_perils_deductible::double precision,
			a.marketid,
			a.group,
			a.company,
			a.dwelling_premium::double precision,
			a.other_structures_premium::double precision,
			a.personal_property_premium::double precision,
			a.loss_of_use_premium::double precision,
			a.liability_premium::double precision,
			a.med_premium::double precision,
			a.deductible_premium::double precision,
			a.dwelling_repl_premium::double precision,
			a.contents_repl_premium::double precision,
			a.hail_premium::double precision,
			a.hurricane_premium::double precision,
			a.fee::double precision,
			a.discount_surcharges_other::double precision,
			a.annualpremium::double precision,
			a.insurancescore,
			a.rated_cova_dwelling,
			a.rated_covb_other_structures,
			a.rated_covc_personal_property,
			a.rated_covd_loss_of_use,
			a.rated_liability_limit,
			a.rated_med_limit,
			a.rated_all_peril_deductible,
			a.covadiff,
			a.covbdiff,
			a.covcdiff,
			a.covddiff,
			a.liabilitydiff,
			a.meddiff,
			a.deductiblediff
		FROM 
		public.ins_home_report_108_recurring a ) combined
	ON trim(combined.state) = trim(m.state) 
	and trim(combined.company) = trim(m.company) 

	LEFT JOIN ( SELECT DISTINCT m_1.geoid,
		m_1.zip,
		m_1.place_state,
		to_number(m1_1.population::text, '9G999g999'::text) AS population,
		m1_1.abbrev,
		m1_1.display_name,
		m1_1.state,
		m1_1.place_tier,
		m1_1.place_tier_rank
	   FROM ins_auto_geo_mapping_2022_new m_1
		 LEFT JOIN ins_auto_geo_mapping_pouplation_2022 m1_1 ON m1_1.place_state::text = m_1.place_state::text
	  WHERE m_1.geo_profile::text = 'Core'::text
		) m1 ON combined.zipcode::text = m1.zip::text

	WHERE combined.rated_cova_dwelling::double precision = combined.cov_a_dwelling::double precision