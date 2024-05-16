
{{ config(materialized='table') }}

WITH CTE AS (
	SELECT 
	to_date(rate_date, 'yyyymmdd') AS data_date,
	state,
	risk_id,
	lpad(zipcode, 5, '0') AS zipcode,
	age_gender_maritalstatus,
	vehicle,
	annual_mileage,
	coverage_level,
	driving_record_violations,
	coalesce(NULLIF(insurancescore_alignment,''), 'Blank') AS insurancescore_alignment,
	age,
	gender,
	marital_status,
	number_incidents,
	number_accidents,
	number_duis,
	number_speedings,
	model_year,
	make,
	model,
	use,
	commute,
	annualmileage,
	ownership,
	residence_type,
	residence_occupancy,
	property_policy_type,
	bi_limit,
	pd_limit,
	comp_deductible,
	coll_deductible,
	marketid,
	"group",
	company,
	CASE WHEN mandatorybi_premium = '0' OR annualpremium = '' OR annualpremium IS NULL THEN 0 ELSE mandatorybi_premium::NUMERIC END AS mandatorybi_premium,
	CASE WHEN bi_pemium = '0' OR bi_pemium = '' OR bi_pemium IS NULL THEN 0 ELSE bi_pemium::NUMERIC END AS bi_pemium,
	CASE WHEN pd_premium = '0' OR pd_premium = '' OR pd_premium IS NULL THEN 0 ELSE pd_premium::NUMERIC END AS pd_premium,
	CASE WHEN umbi_premium = '0' OR umbi_premium = '' OR umbi_premium IS NULL THEN 0 ELSE umbi_premium::NUMERIC END AS umbi_premium,
	CASE WHEN uimbi_premium = '0' OR uimbi_premium = '' OR uimbi_premium IS NULL THEN 0 ELSE uimbi_premium::NUMERIC END AS uimbi_premium,
	CASE WHEN umpd_premium = '0' OR umpd_premium = '' OR umpd_premium IS NULL THEN 0 ELSE umpd_premium::NUMERIC END AS umpd_premium,
	CASE WHEN uimpd_premium = '0' OR uimpd_premium = '' OR uimpd_premium IS NULL THEN 0 ELSE uimpd_premium::NUMERIC END AS uimpd_premium,
	CASE WHEN med_premium = '0' OR med_premium = '' OR med_premium IS NULL THEN 0 ELSE med_premium::NUMERIC END AS med_premium,
	CASE WHEN pip_fpb_preimum = '0' OR pip_fpb_preimum = '' OR pip_fpb_preimum IS NULL THEN 0 ELSE pip_fpb_preimum::NUMERIC END AS pip_fpb_preimum,
	CASE WHEN guestpip_premium = '0' OR guestpip_premium = '' OR guestpip_premium IS NULL THEN 0 ELSE guestpip_premium::NUMERIC END AS guestpip_premium,
	CASE WHEN ppi_premium = '0' OR ppi_premium = '' OR ppi_premium IS NULL THEN 0 ELSE ppi_premium::NUMERIC END AS ppi_premium,
	CASE WHEN comp_premium = '0' OR comp_premium = '' OR comp_premium IS NULL THEN 0 ELSE comp_premium::NUMERIC END AS comp_premium,
	CASE WHEN coll_premium = '0' OR coll_premium = '' OR coll_premium IS NULL THEN 0 ELSE coll_premium::NUMERIC END AS coll_premium,
	CASE WHEN fees = '0' OR fees = '' OR fees IS NULL THEN 0 ELSE fees::NUMERIC END AS fees,
	CASE WHEN discount_surcharge_other = '0' OR discount_surcharge_other = '' OR discount_surcharge_other IS NULL THEN 0 ELSE discount_surcharge_other::NUMERIC END AS discount_surcharge_other,
	CASE WHEN annualpremium = '0' OR annualpremium = '' OR annualpremium IS NULL THEN 0 ELSE annualpremium::NUMERIC END AS annualpremium,
	insurancescore
from ins_auto_report_110_2022_recurring
)

SELECT * from 
CTE

