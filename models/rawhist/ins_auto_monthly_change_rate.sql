{{ config(materialized='table') }}

SELECT t1.*,
	(t1.mandatorybi_premium + t1.med_premium + t1.pip_fpb_preimum + t1.bi_pemium + t1.pd_premium + t1.comp_premium + t1.coll_premium + t1.fees + t1.umbi_premium 
	+ t1.uimbi_premium + t1.umpd_premium + t1.guestpip_premium + t1.ppi_premium + t1.uimpd_premium + t1.discount_surcharge_other ) AS annual_premium_calculated,
	id.provider_id
FROM (
	
	SELECT  
		data_date,
		r.company,
		r.zipcode,
		r.age,
		r.gender,
		r.marital_status,
		r.vehicle,
		r.coverage_level,
		r.insurancescore_alignment,
		r.driving_record_violations,
		p.provider,
		p.national_provider,
		m1.abbrev,
		m1.display_name AS city,
		m1.STATE,
		AVG(annualpremium) as annualpremium,
		AVG(mandatorybi_premium) as mandatorybi_premium,
		AVG(med_premium) as med_premium,
		AVG(pip_fpb_preimum) as pip_fpb_preimum,
		AVG(bi_pemium) as bi_pemium,
		AVG(pd_premium) as pd_premium,
		AVG(comp_premium) as comp_premium,
		AVG(coll_premium) as coll_premium,
		AVG(fees) as fees,
		AVG(discount_surcharge_other) as discount_surcharge_other,
		AVG(umbi_premium) as umbi_premium,
		AVG(uimbi_premium) as uimbi_premium,
		AVG(umpd_premium) as umpd_premium,
		AVG(uimpd_premium) as uimpd_premium,
		AVG(guestpip_premium) as guestpip_premium,
		AVG(ppi_premium) as ppi_premium
	FROM {{ ref('ins_auto_recurring_raw') }} r
	--------------------------- 7/8/2023 Eddy --- Fixed mapping issue with missing provider ---;
	LEFT JOIN (
		SELECT DISTINCT provider,
			national_provider,
			marketid,
			STATE
		FROM new_table_provider_mapping_2023
		) p ON r.marketid::text = p.marketid::text and p.state = r.state
	--------------------add state from zipcode ------------------
	LEFT JOIN (
		SELECT DISTINCT m_1.zip,
			m1_1.abbrev,
			m1_1.display_name,
			m1_1.STATE
		FROM ins_auto_geo_mapping_2022_new m_1
		LEFT JOIN ins_auto_geo_mapping_pouplation_2022 m1_1 ON m1_1.place_state::TEXT = m_1.place_state::TEXT
		WHERE m_1.geo_profile::TEXT = 'Core'::TEXT
		) m1 ON r.zipcode = m1.zip::TEXT
	GROUP BY data_date,
		r.company,
		r.zipcode,
		r.age,
		r.gender,
		r.marital_status,
		r.vehicle,
		r.coverage_level,
		insurancescore_alignment,
		r.driving_record_violations,
		p.provider,
		national_provider,
		m1.abbrev,
		m1.display_name,
		m1.STATE
	) t1
LEFT JOIN (
	SELECT ins_company_collection.sub_vertical,
		ins_company_collection.provider,
		ins_company_collection.national_provider,
		ins_company_collection.provider_id
	FROM ins_company_collection
	WHERE ins_company_collection.sub_vertical = 'Auto Insurance'::TEXT
	) id ON LOWER(id.provider::TEXT) = LOWER(t1.provider::TEXT)