
{{ config(materialized='table') }}

with data_recurring as 
(
	SELECT 
	data_date,
	c_1.company,
	c_1.zipcode,
	c_1.age,
	c_1.gender,
	c_1.marital_status,
	c_1.vehicle,
	c_1.coverage_level,
	c_1.insurancescore_alignment,
	c_1.driving_record_violations,
	provider,
	national_provider,
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
	FROM {{ ref('ins_auto_recurring_raw') }} AS c_1
	--------------------------- 7/08/2023 Eddy --- Fixed mapping issue with missing provider ---;
	LEFT JOIN (
		SELECT DISTINCT provider,
				national_provider,
				marketid,
				state
		FROM new_table_provider_mapping_2023 as t) AS p
		ON c_1.marketid::TEXT = p.marketid::TEXT
		AND p.STATE = c_1.STATE
	WHERE data_date = '2024-02-01'
	AND age::TEXT = '40'::TEXT
	AND c_1.insurancescore_alignment in ('Good','Blank')
	AND vehicle::TEXT = '2012 Toyota Camry LE'::TEXT
	AND driving_record_violations::TEXT = 'Clean'::TEXT
	GROUP BY data_date,
		c_1.company,
		c_1.zipcode,
		c_1.age,
		c_1.gender,
		c_1.marital_status,
		c_1.vehicle,
		c_1.coverage_level,
		c_1.insurancescore_alignment,
		c_1.driving_record_violations,
		provider,
		national_provider
),

--select distinct company,provider, national_provider from data_recurring order by company

data_2022 as (
	SELECT
		-- marketid,
		c_1.company,
		c_1.zipcode,
		STATE,
		c_1.age,
		c_1.gender,
		c_1.marital_status,
		c_1.vehicles,
		c_1.coverage_level,
		c_1.insurancescore_alignment,
		c_1.driving_record_violations,
		provider,
		national_provider,
		avg(NULLIF(annualpremium, 0::NUMERIC)) AS annualpremium,
		avg(NULLIF(annualpremium_mandatory, 0::NUMERIC)) AS annualpremium_mandatory,
		avg(NULLIF(annualpremium_bi_pd, 0::NUMERIC)) AS annualpremium_bi_pd,
		avg(NULLIF(annualpremium_comp_coll, 0::NUMERIC)) AS annualpremium_comp_coll,
		avg(NULLIF(annualpremium_fee_surcharges, 0::NUMERIC)) AS annualpremium_fee_surcharges,
		avg(NULLIF(annualpremium_umbi, 0::NUMERIC)) AS annualpremium_umbi
	FROM ins_auto_raw_data_2022 c_1
	WHERE 
		c_1.quadrant_report::TEXT in ('report_101', 'report_102')
		AND age::TEXT = '40'::TEXT
		AND insurancescore_alignment::TEXT in ('Good','Blank')
		AND vehicles::TEXT = '2012 Toyota Camry LE'::TEXT
		AND driving_record_violations::TEXT = 'Clean'::TEXT
		AND coverage_level IN (
			'100/300/100 w/ $1000 Deductible',
			'State Minimum w/ $500 Deductible'
			)
	GROUP BY c_1.company,
		c_1.zipcode,
		STATE,
		c_1.age,
		c_1.gender,
		c_1.marital_status,
		c_1.vehicles,
		c_1.coverage_level,
		c_1.insurancescore_alignment,
		c_1.driving_record_violations,
		provider,
		national_provider

),data_state_presence as 
(
	SELECT ins_auto_raw_data_2022.national_provider,
		count(DISTINCT ins_auto_raw_data_2022.STATE) AS state_presence
	FROM ins_auto_raw_data_2022
	WHERE ins_auto_raw_data_2022.quadrant_report::TEXT in ('report_101','report_102','report_110')
	GROUP BY ins_auto_raw_data_2022.national_provider
),data_provider as
(
	SELECT ins_company_collection.sub_vertical,
			ins_company_collection.provider,
			ins_company_collection.national_provider,
			ins_company_collection.provider_id
	FROM ins_company_collection
	WHERE ins_company_collection.sub_vertical = 'Auto Insurance'::TEXT
)
SELECT 
	r.company,
	r.data_date,
	r.zipcode,
	STATE,
	r.age,
	r.gender,
	r.marital_status,
	r.vehicle,
	r.coverage_level,
	r.insurancescore_alignment,
	r.driving_record_violations,
	r.provider,
	sp.state_presence,
	r.national_provider,
	CASE WHEN sp.state_presence >= 29 THEN 'Yes'::TEXT ELSE 'No'::TEXT END AS national_presence,
	r.annualpremium AS new_annualpremium,
	c.annualpremium AS core_annualpremium,
	(r.annualpremium - c.annualpremium) / r.annualpremium AS change_premium,
	r.annualpremium / NULLIF(c.annualpremium, 0::NUMERIC) AS perc_annualpremium,
	(r.mandatorybi_premium + r.med_premium + r.pip_fpb_preimum) / NULLIF(c.annualpremium_mandatory, 0::NUMERIC) AS perc_annualpremium_mandatory,
	(r.bi_pemium + r.pd_premium) / NULLIF(c.annualpremium_bi_pd, 0::NUMERIC) AS perc_annualpremium_bi_pd,
	(r.comp_premium + r.coll_premium) / NULLIF(c.annualpremium_comp_coll, 0::NUMERIC) AS perc_annualpremium_comp_coll,
	(r.fees + r.discount_surcharge_other) / NULLIF(c.annualpremium_fee_surcharges, 0::NUMERIC) AS perc_annualpremium_fee_surcharges,
	(r.umbi_premium + r.uimbi_premium + r.umpd_premium + r.uimpd_premium) / NULLIF(c.annualpremium_umbi, 0::NUMERIC) AS perc_annualpremium_umbi,
	(r.mandatorybi_premium + r.med_premium + r.pip_fpb_preimum + r.bi_pemium + r.pd_premium + r.comp_premium + r.coll_premium + r.fees + r.umbi_premium 
		+ r.uimbi_premium + r.umpd_premium + r.guestpip_premium + r.ppi_premium + r.uimpd_premium + r.discount_surcharge_other ) AS annual_premium_calculated,
	id.provider_id
FROM data_recurring r
LEFT JOIN data_2022 c 
	ON r.zipcode::TEXT = c.zipcode::TEXT 
	AND r.company::TEXT = c.company::TEXT
	AND r.age::TEXT = c.age::TEXT
	AND r.gender::TEXT = c.gender::TEXT
	AND r.marital_status::TEXT = c.marital_status::TEXT
	AND r.vehicle::TEXT = c.vehicles::TEXT
	AND r.coverage_level::TEXT = c.coverage_level::TEXT
	AND r.insurancescore_alignment::TEXT = c.insurancescore_alignment::TEXT
	AND r.driving_record_violations::TEXT = c.driving_record_violations::TEXT
LEFT JOIN data_state_presence sp
	ON btrim(c.national_provider::TEXT) = btrim(sp.national_provider::TEXT)
LEFT JOIN data_provider id 
	ON LOWER(r.provider::TEXT) = LOWER(id.provider::TEXT)
