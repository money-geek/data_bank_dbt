{{ config(materialized='table') }}

with recent_date as (
SELECT data_date, zipcode, company, age, gender, marital_status, vehicle, coverage_level, insurancescore_alignment, driving_record_violations, provider, national_provider, city, state, annual_premium_calculated
FROM ins_auto_monthly_change_rate 
WHERE data_date IN (SELECT DISTINCT data_date FROM ins_auto_monthly_change_rate ORDER BY data_date DESC LIMIT 1)
), comparison_date as (
SELECT 
	rd.data_date as recent_date,
	cd.data_date as previous_date,
	cd.company, 
	cd.zipcode,
	cd.age, 
	cd.gender, 
	cd.marital_status, 
	cd.vehicle, 
	cd.coverage_level, 
	cd.insurancescore_alignment, 
	cd.driving_record_violations, 
	cd.provider, 
	cd.national_provider, 
	cd.city, 
	cd.state, 
	rd.annual_premium_calculated as recent_annual_premium,
	cd.annual_premium_calculated as previous_annual_premium
FROM {{ ref('ins_auto_monthly_change_rate') }} cd
INNER JOIN recent_date rd ON cd.company = rd.company AND cd.age = rd.age AND cd.zipcode = rd.zipcode AND cd.gender = rd.gender 
AND cd.marital_status = rd.marital_status AND cd.vehicle = rd.vehicle AND cd.coverage_level = rd.coverage_level AND cd.insurancescore_alignment = rd.insurancescore_alignment
AND cd.driving_record_violations = rd.driving_record_violations AND cd.provider = rd.provider AND cd.national_provider = rd.national_provider
AND cd.city = rd.city AND cd.state = rd.state
AND cd.annual_premium_calculated != rd.annual_premium_calculated
WHERE cd.data_date IN (SELECT DISTINCT data_date FROM ins_auto_monthly_change_rate ORDER BY data_date DESC LIMIT 2)
AND cd.data_date NOT IN (SELECT DISTINCT data_date FROM ins_auto_monthly_change_rate ORDER BY data_date DESC LIMIT 1)
)
SELECT DISTINCT * 
FROM comparison_date