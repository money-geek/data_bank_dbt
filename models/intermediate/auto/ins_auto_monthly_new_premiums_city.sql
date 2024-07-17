
{{ config(materialized='table') }}

WITH cte AS (select qm.*,
		COALESCE(r.stability_score, n_avg.national_stability_5) AS stability_score,
        COALESCE(r.satisfaction_score, n_avg.national_satisfaction_5) AS satisfaction_score,
        COALESCE(r.claims_score, n_avg.national_claims_5) AS claims_score,
        COALESCE(r.coverage_score, n_avg.national_coverage_5) AS coverage_score,
		id.provider_id
    from (
        SELECT
        r.zipcode,
        CASE
        WHEN r.display_name = 'Arden-Arcade' THEN 'Arden'
        WHEN r.display_name = 'Athens-Clarke' THEN 'Athens'
        WHEN r.display_name = 'Augusta-Richmond' THEN 'Augusta'
        WHEN r.display_name = 'Lexington-Fayette' THEN 'Lexington'
        WHEN r.display_name = 'Louisville/Jefferson' THEN 'Louisville'
        WHEN r.display_name = 'Macon-Bibb' THEN 'Macon'
        WHEN r.display_name = 'Nashville-Davidson' THEN 'Nashville'
        WHEN r.display_name = 'Urban Honolulu' THEN 'Honolulu'
        WHEN r.display_name = 'Boise City' THEN 'Boise'
        ELSE r.display_name
        END as city,
        case 
            when c.data_date is null 
            then (select distinct data_date 
                     from {{ ref('ins_auto_monthly_change') }} where data_date is not null )
            else c.data_date end AS data_date,
        r.place_state,
        r.display_name,
        r.abbrev AS state_code,
        r.state,
        p.provider,
        p.national_provider,
        r.age,
        r.gender,
        r.marital_status,
        r.drv2_age,
        r.drv2_gender,
        r.drv2_marital_status,
        r.drv3_age,
        r.drv3_gender,
        r.drv3_marital_status,
        r.vehicles,
        r.driving_record_violations,
        r.insurancescore_alignments as insurancescore_alignment,
        r.coverage_level,
        r.number_incidents,
        r.number_accidents,
        r.number_duis,
        r.number_speedings,
        r.model_year,
        r.make,
        r.model,
        r.use,
        r.commute,
        r.annualmileage,
        r.ownership,
        r.residence_type,
        r.residence_occupancy,
        r.property_policy_type,
        r.bi_pd_limit,
        r.umbi_limit,
        r.comp_coll_deductible,
        r.is_user_a_veteran,
    --  r.quadrant_report,
    --    r.population,
        r.abbrev,
    --  r.state_population,
    --    r.place_tier,
    --    r.place_tier_rank,
        avg( r.quadrant_annualpremium * COALESCE(c.perc_annualpremium, c1.perc_annualpremium)) AS annualpremium,
        avg( r.annualpremium_mandatory * COALESCE(c.perc_annualpremium_mandatory, c1.perc_annualpremium_mandatory) )AS annualpremium_mandatory,
        avg( r.annualpremium_bi_pd * COALESCE(c.perc_annualpremium_bi_pd, c1.perc_annualpremium_bi_pd) )AS annualpremium_bi_pd,
        avg(  r.annualpremium_comp_coll * COALESCE(c.perc_annualpremium_comp_coll, c1.perc_annualpremium_comp_coll)) AS annualpremium_comp_coll,
        avg(   r.annualpremium_umbi * COALESCE(c.perc_annualpremium_umbi, c1.perc_annualpremium_umbi) )AS annualpremium_umbi,
        avg(  r.annualpremium_fee_surcharges * COALESCE(c.perc_annualpremium_fee_surcharges, c1.perc_annualpremium_fee_surcharges) )AS annualpremium_fee_surcharges,
        avg( r.quadrant_annualpremium * COALESCE(c.perc_annualpremium, c1.perc_annualpremium) * r.population / r.state_population) AS wt_rate,
        avg(  r.annualpremium_mandatory * COALESCE(c.perc_annualpremium_mandatory, c1.perc_annualpremium_mandatory) * r.population / r.state_population )AS wt_rate_mandatory,
        avg(  r.annualpremium_bi_pd * COALESCE(c.perc_annualpremium_bi_pd, c1.perc_annualpremium_bi_pd) * r.population / r.state_population) AS wt_rate_bi_pd,
        avg( r.annualpremium_comp_coll * COALESCE(c.perc_annualpremium_comp_coll, c1.perc_annualpremium_comp_coll) * r.population / r.state_population )AS wt_rate_comp_coll,
        avg(  r.annualpremium_umbi * COALESCE(c.perc_annualpremium_umbi, c1.perc_annualpremium_umbi) * r.population / r.state_population )AS wt_rate_umbi,
        avg(  r.annualpremium_fee_surcharges * COALESCE(c.perc_annualpremium_fee_surcharges, c1.perc_annualpremium_fee_surcharges) * r.population / r.state_population) AS wt_rate_fees
    FROM (select *,case when insurancescore_alignment = 'Blank' then 'Good' else insurancescore_alignment end as insurancescore_alignments 
            from ins_auto_raw_data_2022) r
    LEFT JOIN
            (select  data_date ,zipcode , company ,coverage_level ,
                avg(perc_annualpremium) as perc_annualpremium,
                avg(perc_annualpremium_mandatory) as perc_annualpremium_mandatory,
                avg(perc_annualpremium_bi_pd) as perc_annualpremium_bi_pd,
                avg(perc_annualpremium_comp_coll) as perc_annualpremium_comp_coll,
                avg(perc_annualpremium_umbi) as perc_annualpremium_umbi,
                avg(perc_annualpremium_fee_surcharges) as perc_annualpremium_fee_surcharges
            from {{ ref('ins_auto_monthly_change') }}
            group by  data_date, zipcode , company ,coverage_level
            )  c ON r.zipcode::text = c.zipcode::text AND r.company::text = c.company::text AND r.coverage_level::text = c.coverage_level::text
    LEFT JOIN ( SELECT 
                    data_date,
                    ins_auto_monthly_change.zipcode,
                    ins_auto_monthly_change.company,
                    '300/500/300 w/ $1500 Deductible'::text AS coverage_level,
                    avg(perc_annualpremium) as perc_annualpremium,
                    avg(perc_annualpremium_mandatory) as perc_annualpremium_mandatory,
                    avg(perc_annualpremium_bi_pd) as perc_annualpremium_bi_pd,
                    avg(perc_annualpremium_comp_coll) as perc_annualpremium_comp_coll,
                    avg(perc_annualpremium_umbi) as perc_annualpremium_umbi,
                    avg(perc_annualpremium_fee_surcharges) as perc_annualpremium_fee_surcharges
                FROM {{ ref('ins_auto_monthly_change') }}            
                WHERE ins_auto_monthly_change.coverage_level::text = '100/300/100 w/ $1000 Deductible'::text
                    group by   data_date,zipcode , company ,coverage_level
                ) c1 
    ON r.zipcode::text = c1.zipcode::text AND r.company::text = c1.company::text AND r.coverage_level::text = c1.coverage_level
    LEFT JOIN ( SELECT DISTINCT provider, national_provider, marketid, state
        from new_table_provider_mapping_2023
        ) p ON r.marketid::text = p.marketid::text and p.state = r.abbrev

    WHERE r.quadrant_report::text in ('report_101', 'report_102', 'report_110')
    --and r.zipcode = '01118' and r.company = 'Progressive Direct Ins Co'
    group by   c.data_date,r.zipcode,display_name,   place_state, display_name, abbrev, r.state, p.national_provider, p.provider, age, gender, marital_status, drv2_age, drv2_gender, drv2_marital_status, drv3_age, 
        drv3_gender, drv3_marital_status, vehicles, driving_record_violations, insurancescore_alignments, r.coverage_level, number_incidents, number_accidents, number_duis, 
        number_speedings, model_year, make, model, use, commute, annualmileage, ownership, residence_type, residence_occupancy, property_policy_type,
        bi_pd_limit, umbi_limit, comp_coll_deductible, is_user_a_veteran
    ORDER BY r.driving_record_violations, r.age) qm

    LEFT JOIN ins_auto_scores_2023 r 
        ON btrim(qm.state::text) = btrim(r.state::text) AND btrim(qm.provider::text) = btrim(r.provider::text)

    LEFT JOIN ( SELECT ins_auto_rating_state.provider,
                        avg(ins_auto_rating_state.stability_score) AS national_stability_5,
                        avg(ins_auto_rating_state.satisfaction_score) AS national_satisfaction_5,
                        avg(ins_auto_rating_state.claims_score) AS national_claims_5,
                        avg(ins_auto_rating_state.coverage_score) AS national_coverage_5
                    FROM ins_auto_scores_2023 ins_auto_rating_state
                    GROUP BY ins_auto_rating_state.provider) n_avg ON trim(qm.provider::text) = trim(n_avg.provider::text)
    LEFT JOIN ( SELECT DISTINCT ins_company_collection.sub_vertical,
                ins_company_collection.provider,
                ins_company_collection.provider_id
            FROM ins_company_collection
            WHERE ins_company_collection.sub_vertical = 'Auto Insurance'::text) id 
            ON LOWER(id.provider::text) = LOWER(trim(qm.provider::text))
    )


SELECT * FROM cte WHERE annualpremium > 0