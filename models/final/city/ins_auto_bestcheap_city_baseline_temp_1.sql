
    {{ config(materialized='table') }}
    
    SELECT c.data_date,
    c.state,
    c.city,
    c.age,
    c.provider,provider_id,
	case when c.driving_record_violations = 'DUI - BAC >= .08' then 'DUI'
		 when c.driving_record_violations = 'Clean' then 'Clean'
		 when c.driving_record_violations = 'Speeding 11-15 MPH > Limit' then 'Speeding'
		 when c.driving_record_violations = 'Accident $1000-$1999 Prop Dmg' then 'Accident'
		 else c.driving_record_violations end as driving_record_violations,
	c.is_user_a_veteran,
	c.insurancescore_alignment,
	c.vehicles,
    c.coverage,
    c.stability_score,
    c.satisfaction_score,
    c.claims_score,
    c.coverage_score,
    c.affordability_score,
    c.annual_premium,
    c.monthly_premium,
	stability_score_05,
	satisfaction_score_35,
	claims_score_20,
	coverage_score_10,
	affordability_score_30,
	mg_total_best_100,
	rank_state_best,
	stability_rank,
	satisfaction_rank,
	claims_rank,
	coverage_rank,
	affordability_rank,
        CASE
            WHEN c.age::text = '25'::text THEN '22-29'::text
            WHEN c.age::text = '40'::text THEN '30-59'::text
            WHEN c.age::text = '65'::text THEN '60 +'::text
            ELSE NULL::text
        END AS age_range,
    dense_rank() OVER (PARTITION BY c.data_date, c.age, c.state, c.city, c.coverage,c.driving_record_violations,c.insurancescore_alignment,c.vehicles ORDER BY c.annual_premium) AS cheapest_rank
   FROM ( WITH cte_1 AS (
                 SELECT t.data_date,
                    t.age,
                    t.provider,provider_id,
	   				t.driving_record_violations,
	   				t.is_user_a_veteran,
	   				t.insurancescore_alignment,
	   
	   				t.vehicles,
                    t.state,
                    t.city,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.bi_pd,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score
                   FROM ( SELECT data_date,
                            ins_auto_master_table_city_2022.state,
                            ins_auto_master_table_city_2022.city,
                            ins_auto_master_table_city_2022.age,
                            ins_auto_master_table_city_2022.provider,provider_id,
						 	ins_auto_master_table_city_2022.driving_record_violations,
						 	ins_auto_master_table_city_2022.is_user_a_veteran,
						 	ins_auto_master_table_city_2022.insurancescore_alignment,
						 	ins_auto_master_table_city_2022.vehicles,
                            'Full Coverage'::text AS coverage,
                            avg(ins_auto_master_table_city_2022.annualpremium_mandatory) AS state_pop_wt_rate_mandatory,--
                            avg(ins_auto_master_table_city_2022.annualpremium_comp_coll) AS state_pop_wt_rate_comp_coll,--
                            avg(
                                CASE
                                    WHEN ins_auto_master_table_city_2022.state::text <> ALL (ARRAY['Connecticut'::character varying::text, 'Illinois'::character varying::text, 'Kansas'::character varying::text, 'Maine'::character varying::text, 'Maryland'::character varying::text, 'Massachusetts'::character varying::text, 'Minnesota'::character varying::text, 'Missouri'::character varying::text, 'Nebraska'::character varying::text, 'New Hampshire'::character varying::text, 'New York'::character varying::text, 'North Carolina'::character varying::text, 'North Dakota'::character varying::text, 'Oregon'::character varying::text, 'South Carolina'::character varying::text, 'South Dakota'::character varying::text, 'Vermont'::character varying::text, 'Virginia'::character varying::text, 'District of Columbia'::character varying::text, 'West Virginia'::character varying::text, 'Wisconsin'::character varying::text]) THEN 0::numeric
                                    ELSE ins_auto_master_table_city_2022.annualpremium_umbi--
                                END) AS state_pop_wt_rate_umbi,
                            avg(ins_auto_master_table_city_2022.annualpremium_fee_surcharges) AS state_pop_wt_rate_fees,--
                            avg(ins_auto_master_table_city_2022.annualpremium_bi_pd) AS bi_pd,--
                            avg(ins_auto_master_table_city_2022.stability_score) AS stability_score,
                            avg(ins_auto_master_table_city_2022.satisfaction_score) AS satisfaction_score,
                            avg(ins_auto_master_table_city_2022.claims_score) AS claims_score,
                            avg(ins_auto_master_table_city_2022.coverage_score) AS coverage_score
                           FROM {{ ref('ins_auto_monthly_new_premiums_city_full_inserted') }} ins_auto_master_table_city_2022
                          WHERE data_date = '2024-01-01' 
                          AND (ins_auto_master_table_city_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_city_2022.comp_coll_deductible = '1000/1000'::text AND ins_auto_master_table_city_2022.bi_pd_limit = '100/300/100000'::text AND ins_auto_master_table_city_2022.is_user_a_veteran = 'No'::text
                          GROUP BY data_date,ins_auto_master_table_city_2022.vehicles,ins_auto_master_table_city_2022.insurancescore_alignment,ins_auto_master_table_city_2022.is_user_a_veteran,ins_auto_master_table_city_2022.driving_record_violations, ins_auto_master_table_city_2022.age, ins_auto_master_table_city_2022.state, ins_auto_master_table_city_2022.city, ins_auto_master_table_city_2022.provider,provider_id, 'Full Coverage'::text) t
                UNION
                 SELECT t.data_date,
                    t.age,
                    t.provider,provider_id,
	   				t.driving_record_violations,
	   				t.is_user_a_veteran,
	   				t.insurancescore_alignment,
	   				t.vehicles,
                    t.state,
                    t.city,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.bi_pd,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score
                   FROM ( SELECT data_date,
                            ins_auto_master_table_city_2022.state,
                            ins_auto_master_table_city_2022.city,
                            ins_auto_master_table_city_2022.age,
                            ins_auto_master_table_city_2022.provider,provider_id,
						 	ins_auto_master_table_city_2022.driving_record_violations,
						 	ins_auto_master_table_city_2022.is_user_a_veteran,
						 	ins_auto_master_table_city_2022.insurancescore_alignment,
						 	ins_auto_master_table_city_2022.vehicles,
                            'Minimum Coverage'::text AS coverage,
                            avg(ins_auto_master_table_city_2022.annualpremium_mandatory) AS state_pop_wt_rate_mandatory,
                            0 AS state_pop_wt_rate_comp_coll,
                            avg(
                                CASE
                                    WHEN ins_auto_master_table_city_2022.state::text <> ALL (ARRAY['Connecticut'::character varying::text, 'Illinois'::character varying::text, 'Kansas'::character varying::text, 'Maine'::character varying::text, 'Maryland'::character varying::text, 'Massachusetts'::character varying::text, 'Minnesota'::character varying::text, 'Missouri'::character varying::text, 'Nebraska'::character varying::text, 'New Hampshire'::character varying::text, 'New York'::character varying::text, 'North Carolina'::character varying::text, 'North Dakota'::character varying::text, 'Oregon'::character varying::text, 'South Carolina'::character varying::text, 'South Dakota'::character varying::text, 'Vermont'::character varying::text, 'Virginia'::character varying::text, 'District of Columbia'::character varying::text, 'West Virginia'::character varying::text, 'Wisconsin'::character varying::text]) THEN 0::numeric
                                    ELSE ins_auto_master_table_city_2022.annualpremium_umbi
                                END) AS state_pop_wt_rate_umbi,
                            avg(ins_auto_master_table_city_2022.annualpremium_fee_surcharges) AS state_pop_wt_rate_fees,
                            avg(ins_auto_master_table_city_2022.annualpremium_bi_pd) AS bi_pd,
                            avg(ins_auto_master_table_city_2022.stability_score) AS stability_score,
                            avg(ins_auto_master_table_city_2022.satisfaction_score) AS satisfaction_score,
                            avg(ins_auto_master_table_city_2022.claims_score) AS claims_score,
                            avg(ins_auto_master_table_city_2022.coverage_score) AS coverage_score
                           FROM {{ ref('ins_auto_monthly_new_premiums_city_full_inserted') }} ins_auto_master_table_city_2022
                          WHERE  data_date = '2024-01-01' --'{{ var("current_month") }}' 
                          and (ins_auto_master_table_city_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_city_2022.bi_pd_limit = 'State Minimum'::text AND ins_auto_master_table_city_2022.is_user_a_veteran = 'No'::text
                          GROUP BY data_date,ins_auto_master_table_city_2022.vehicles,ins_auto_master_table_city_2022.insurancescore_alignment,ins_auto_master_table_city_2022.is_user_a_veteran,ins_auto_master_table_city_2022.driving_record_violations, ins_auto_master_table_city_2022.age, ins_auto_master_table_city_2022.state, ins_auto_master_table_city_2022.city, ins_auto_master_table_city_2022.provider,provider_id, 'Minimum Coverage'::text) t
                ), cte_2 AS (
                 SELECT cte_1.data_date,
                    cte_1.state,
                    cte_1.city,
                    cte_1.age,
                    cte_1.provider,provider_id,
					cte_1.driving_record_violations,
					cte_1.is_user_a_veteran,
					cte_1.insurancescore_alignment,
					cte_1.vehicles,
                    cte_1.coverage,
                    cte_1.stability_score,
                    cte_1.satisfaction_score,
                    cte_1.claims_score,
                    cte_1.coverage_score,
                    --cte_1.state_pop_wt_rate_mandatory + cte_1.state_pop_wt_rate_comp_coll + cte_1.state_pop_wt_rate_umbi + cte_1.state_pop_wt_rate_fees + cte_1.bi_pd AS annual_premium,
                    --(cte_1.state_pop_wt_rate_mandatory + (+ cte_1.state_pop_wt_rate_comp_coll) + cte_1.state_pop_wt_rate_umbi + cte_1.state_pop_wt_rate_fees + cte_1.bi_pd) / 12::numeric AS monthly_premium
					COALESCE(cte_1.state_pop_wt_rate_mandatory, 0) + COALESCE(cte_1.state_pop_wt_rate_comp_coll, 0) + COALESCE(cte_1.state_pop_wt_rate_umbi, 0) + COALESCE(cte_1.state_pop_wt_rate_fees, 0) + COALESCE(cte_1.bi_pd, 0) AS annual_premium,
					(COALESCE(cte_1.state_pop_wt_rate_mandatory, 0) + COALESCE(cte_1.state_pop_wt_rate_comp_coll, 0) + COALESCE(cte_1.state_pop_wt_rate_umbi, 0) + COALESCE(cte_1.state_pop_wt_rate_fees, 0) + COALESCE(cte_1.bi_pd, 0)) / 12::numeric AS monthly_premium

                   FROM cte_1
                ), cte_min AS (
                 SELECT cte_2.data_date,
                    cte_2.age,
                    cte_2.state,
                    cte_2.coverage,
					cte_2.driving_record_violations,
					cte_2.insurancescore_alignment,
					cte_2.vehicles,
                    cte_2.city,
                    min(cte_2.annual_premium) AS min_premium,
                    max(cte_2.annual_premium) AS max_premium
                   FROM cte_2
                  GROUP BY cte_2.data_date, cte_2.age, cte_2.coverage, cte_2.state, cte_2.city,cte_2.driving_record_violations,cte_2.insurancescore_alignment,cte_2.vehicles
                ), cte_3 AS (
                 SELECT a.data_date,
                    a.city,
                    a.state,
                    a.age,
                    a.provider,provider_id,
					a.driving_record_violations,
					a.is_user_a_veteran,
					a.insurancescore_alignment,
					a.vehicles,
                    a.coverage,
                    a.stability_score,
                    a.satisfaction_score,
                    a.claims_score,
                    a.coverage_score,
                    a.annual_premium,
                    a.monthly_premium,
                    m.min_premium,
                    m.max_premium,
					case when (m.max_premium-m.min_premium) = 0 then 5 
                	else (1::numeric - (a.annual_premium - m.min_premium) / (m.max_premium - m.min_premium)) * 5::numeric end AS affordability_score
                   FROM cte_2 a
                     LEFT JOIN cte_min m ON a.data_date = m.data_date AND a.age::text = m.age::text 
								AND a.coverage = m.coverage AND a.city::text = m.city::text 
								AND a.state::text = m.state::text AND a.driving_record_violations::text = m.driving_record_violations::text
								AND a.insurancescore_alignment::text = m.insurancescore_alignment::text 
								AND a.vehicles::text = m.vehicles::text 
                ),cte_scores as (
			select 
				cte_3.data_date,
                cte_3.age,
                cte_3.state,
                cte_3.coverage,
				cte_3.driving_record_violations,
				cte_3.insurancescore_alignment,
				cte_3.vehicles,
                cte_3.city,
				min(stability_score) as min_stability_score,
            	min(satisfaction_score) as min_satisfaction_score,
           		min(claims_score) as min_claims_score,
            	min(coverage_score) as min_coverage_score,
            	min(affordability_score) as min_affordability_score,
				max(stability_score) as max_stability_score,
            	max(satisfaction_score) as max_satisfaction_score,
           		max(claims_score) as max_claims_score,
            	max(coverage_score) as max_coverage_score,
            	max(affordability_score) as max_affordability_score
			from cte_3
            GROUP BY cte_3.data_date, cte_3.age, cte_3.coverage, cte_3.state, cte_3.city,cte_3.driving_record_violations,cte_3.insurancescore_alignment,cte_3.vehicles
		), cte_score AS (
                 SELECT a.data_date,
                    a.city,
                    a.state,
                    a.age,
                    a.provider,provider_id,
					a.driving_record_violations,
					a.is_user_a_veteran,
					a.insurancescore_alignment,
					a.vehicles,
                    a.coverage,
                    case when a.stability_score is null then 'No Score' 
					else round(a.stability_score::int,2) :: text end as stability_score ,
					case when a.satisfaction_score is null then 'No Score'
					else round(a.satisfaction_score::int,2) :: text end as satisfaction_score ,
					case when a.claims_score is null then 'No Score'
					else round(a.claims_score::int,2) :: text end as claims_score,  
					case when a.coverage_score is null then 'No Score'
					else round(a.coverage_score::int,2) :: text  end as coverage_score,
                    a.affordability_score,
                    a.annual_premium,
                    a.monthly_premium,
					case when (max_stability_score-min_stability_score) = 0 then 5
					else 
					((stability_score - cte_scores.min_stability_score)/(cte_scores.max_stability_score - cte_scores.min_stability_score) * (5-1.6))+1.6 end as stability_score_05,
					case when (max_satisfaction_score-min_satisfaction_score) = 0 then 35
					else 
					((satisfaction_score - cte_scores.min_satisfaction_score)/(cte_scores.max_satisfaction_score - cte_scores.min_satisfaction_score) * (35-11.6))+11.6 end as satisfaction_score_35,
					case when (max_claims_score-min_claims_score) = 0 then 20
					else 
					((claims_score - cte_scores.min_claims_score)/(cte_scores.max_claims_score - cte_scores.min_claims_score) * (20-6.6))+6.6 end as claims_score_20,
					case when (max_coverage_score-min_coverage_score) = 0 then 10
					else 
					((coverage_score - cte_scores.min_coverage_score)/(cte_scores.max_coverage_score - cte_scores.min_coverage_score) * (10-3.3))+3.3 end as coverage_score_10,
					case when (max_affordability_score-min_affordability_score) = 0 then 30
					else 
					((affordability_score - cte_scores.min_affordability_score)/(cte_scores.max_affordability_score - cte_scores.min_affordability_score) * (30-10))+10 end as affordability_score_30
                   	FROM cte_3 a
					left join cte_scores 
								ON a.data_date = cte_scores.data_date AND a.age::text = cte_scores.age::text 
								AND a.coverage = cte_scores.coverage AND a.city::text = cte_scores.city::text 
								AND a.state::text = cte_scores.state::text AND a.driving_record_violations::text = cte_scores.driving_record_violations::text
								AND a.insurancescore_alignment::text = cte_scores.insurancescore_alignment::text 
								AND a.vehicles::text = cte_scores.vehicles::text 
                ), cte_best AS (
                 SELECT a.data_date::date AS data_date,
                    a.state,
                    a.city,
                    a.age,
                    a.provider,provider_id,
					a.driving_record_violations,
					a.is_user_a_veteran,
					a.insurancescore_alignment,
					a.vehicles,
                    a.coverage,
                    a.stability_score,
                    a.satisfaction_score,
                    a.claims_score,
                    a.coverage_score,
                    a.affordability_score,
                    a.annual_premium,
                    a.monthly_premium,
					stability_score_05,
					satisfaction_score_35,
					claims_score_20,
					coverage_score_10,
					affordability_score_30,
					dense_rank() OVER (PARTITION BY a.data_date, a.age, a.state, a.city, a.coverage,a.driving_record_violations,a.insurancescore_alignment,a.vehicles ORDER BY stability_score_05 DESC NULLS LAST)::varchar AS stability_rank,
					dense_rank() OVER (PARTITION BY a.data_date, a.age, a.state, a.city, a.coverage,a.driving_record_violations,a.insurancescore_alignment,a.vehicles ORDER BY satisfaction_score_35 DESC NULLS LAST)::varchar AS satisfaction_rank,
					dense_rank() OVER (PARTITION BY a.data_date, a.age, a.state, a.city, a.coverage,a.driving_record_violations,a.insurancescore_alignment,a.vehicles ORDER BY claims_score_20 DESC NULLS LAST)::varchar AS claims_rank,
					dense_rank() OVER (PARTITION BY a.data_date, a.age, a.state, a.city, a.coverage,a.driving_record_violations,a.insurancescore_alignment,a.vehicles ORDER BY coverage_score_10 DESC NULLS LAST)::varchar AS coverage_rank,
					dense_rank() OVER (PARTITION BY a.data_date, a.age, a.state, a.city, a.coverage,a.driving_record_violations,a.insurancescore_alignment,a.vehicles ORDER BY affordability_score_30 DESC NULLS LAST)::varchar AS affordability_rank,
                    round((stability_score_05 +  satisfaction_score_35 + claims_score_20 +coverage_score_10 + affordability_score_30)::int,2)::varchar as mg_total_best_100,
                    dense_rank() OVER (PARTITION BY a.data_date, a.age, a.state, a.city, a.coverage,a.driving_record_violations,a.insurancescore_alignment,a.vehicles ORDER BY stability_score_05 + 
							  satisfaction_score_35 + claims_score_20 +coverage_score_10 + affordability_score_30 DESC NULLS LAST)::varchar AS rank_state_best
                   FROM cte_score a               
                )
         SELECT cte_best.data_date,
            cte_best.state,
            cte_best.city,
            cte_best.age,
            cte_best.provider,provider_id,
		 	cte_best.driving_record_violations,
		 	cte_best.is_user_a_veteran,
		 	cte_best.insurancescore_alignment,
		 	cte_best.vehicles,
            cte_best.coverage,
            cte_best.stability_score,
            cte_best.satisfaction_score,
            cte_best.claims_score,
            cte_best.coverage_score,
            cte_best.affordability_score,
            cte_best.annual_premium,
            cte_best.monthly_premium,
            stability_score_05,
			satisfaction_score_35,
			claims_score_20,
			coverage_score_10,
			affordability_score_30,
			case when mg_total_best_100 is null then 'No Score'
			else mg_total_best_100 end as mg_total_best_100,
			case when mg_total_best_100 is null then 'No Score'
			else rank_state_best end as rank_state_best,
			case when stability_score_05 is null then 'No Score'
			else stability_rank end as stability_rank,
			case when satisfaction_score_35 is null then 'No Score'
			else satisfaction_rank end as satisfaction_rank,
			case when claims_score_20 is null then 'No Score'
			else claims_rank end as claims_rank,
			case when coverage_score_10 is null then 'No Score'
			else coverage_rank end as coverage_rank,
			case when affordability_score_30 is null then 'No Score'
			else affordability_rank end as affordability_rank
           FROM cte_best) c 
	
	