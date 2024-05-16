

{{ config(materialized='table') }}

select     a_1.data_date,
			a_1.state,
			a_1.state_code,
			a_1.national_provider,
			a_1.provider,
			a_1.national_presence,
			a_1.age,
			a_1.gender,
			a_1.marital_status,
			a_1.drv2_age,
			a_1.drv2_gender,
			a_1.drv2_marital_status,
			a_1.drv3_age,
			a_1.drv3_gender,
			a_1.drv3_marital_status,
			a_1.vehicles,
			a_1.driving_record_violations,
			a_1.insurancescore_alignment,
			a_1.coverage_level,
			a_1.number_incidents,
			a_1.number_accidents,
			a_1.number_duis,
			a_1.number_speedings,
			a_1.model_year,
			a_1.make,
			a_1.model,
			a_1.use,
			a_1.commute,
			a_1.annualmileage,
			a_1.ownership,
			a_1.residence_type,
			a_1.residence_occupancy,
			a_1.property_policy_type,
			a_1.bi_pd_limit,
			a_1.umbi_limit,
			a_1.comp_coll_deductible,
			a_1.is_user_a_veteran,
			a_1.state_annualpremium,
			a_1.state_pop_wt_rate_mandatory,
			a_1.state_pop_wt_rate_bi_pd,
			a_1.state_pop_wt_rate_comp_coll,
			a_1.state_pop_wt_rate_umbi,
			a_1.state_pop_wt_rate_fees,
		id.provider_id,
		COALESCE(r.stability_score, n_avg.national_stability_5)::numeric AS stability_score,
        COALESCE(r.satisfaction_score, n_avg.national_satisfaction_5)::numeric AS satisfaction_score,
        COALESCE(r.claims_score, n_avg.national_claims_5)::numeric AS claims_score,
        COALESCE(r.coverage_score, n_avg.national_coverage_5)::numeric AS coverage_score
from {{ ref('ins_auto_pop_wt_new_premiums') }}  a_1
LEFT JOIN ( SELECT ins_auto_rating_state.provider,
                                    avg(ins_auto_rating_state.stability_score) AS national_stability_5,
                                    avg(ins_auto_rating_state.satisfaction_score) AS national_satisfaction_5,
                                    avg(ins_auto_rating_state.claims_score) AS national_claims_5,
                                    avg(ins_auto_rating_state.coverage_score) AS national_coverage_5
                                   FROM ins_auto_scores_2023 ins_auto_rating_state
                                  GROUP BY ins_auto_rating_state.provider) n_avg ON btrim(a_1.provider::text) = btrim(n_avg.provider::text)
LEFT JOIN ins_auto_scores_2023 r ON btrim(a_1.state_code::text) = btrim(r.state::text) AND btrim(a_1.provider::text) = btrim(r.provider::text)
LEFT JOIN ( SELECT ins_company_collection.sub_vertical,
            ins_company_collection.provider,
            ins_company_collection.national_provider,
            ins_company_collection.provider_id
           FROM ins_company_collection
          WHERE ins_company_collection.sub_vertical = 'Auto Insurance'::text) id ON lower(id.provider::text) = lower(a_1.provider::text)