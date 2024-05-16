 {{ config(materialized='table') }}

WITH cte_1 AS (
         SELECT-- TO_CHAR(ins_auto_master_table_2022.data_date, 'MM/DD/YYYY') as data_date,
            ins_auto_master_table_2022.data_date,
            ins_auto_master_table_2022.state,
            ins_auto_master_table_2022.age,
            ins_auto_master_table_2022.provider,
            'Full Coverage'::text AS coverage,
            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd,
            avg(ins_auto_master_table_2022.state_pop_wt_rate_comp_coll) AS state_pop_wt_rate_comp_coll,
            avg(
                CASE
                    WHEN ins_auto_master_table_2022.state::text <> ALL (ARRAY['Connecticut'::character varying::text, 'Illinois'::character varying::text, 'Kansas'::character varying::text, 'Maine'::character varying::text, 'Maryland'::character varying::text, 'Massachusetts'::character varying::text, 'Minnesota'::character varying::text, 'Missouri'::character varying::text, 'Nebraska'::character varying::text, 'New Hampshire'::character varying::text, 'New York'::character varying::text, 'North Carolina'::character varying::text, 'North Dakota'::character varying::text, 'Oregon'::character varying::text, 'South Carolina'::character varying::text, 'South Dakota'::character varying::text, 'Vermont'::character varying::text, 'Virginia'::character varying::text, 'District of Columbia'::character varying::text, 'West Virginia'::character varying::text, 'Wisconsin'::character varying::text]) THEN 0::numeric
                    ELSE ins_auto_master_table_2022.state_pop_wt_rate_umbi
                END) AS state_pop_wt_rate_umbi,
            avg(ins_auto_master_table_2022.state_pop_wt_rate_fees) AS state_pop_wt_rate_fees,
            avg(ins_auto_master_table_2022.stability_score) AS stability_score,
            avg(ins_auto_master_table_2022.satisfaction_score) AS satisfaction_score,
            avg(ins_auto_master_table_2022.claims_score) AS claims_score,
            avg(ins_auto_master_table_2022.coverage_score) AS coverage_score
           FROM {{ ref('ins_auto_master_table_state_2023_temp') }} ins_auto_master_table_2022
          WHERE ins_auto_master_table_2022.provider != 'USAA' 
      AND (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) 
      AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text
      AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) 
      AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text  AND ins_auto_master_table_2022.bi_pd_limit = '100/300/100000'::text 
      AND ins_auto_master_table_2022.comp_coll_deductible = '1000/1000'::text
          GROUP BY data_date, ins_auto_master_table_2022.state, ins_auto_master_table_2022.age, ins_auto_master_table_2022.provider, 'Full Coverage'::text
        ), cte_2 AS (
         SELECT cte_1.data_date,
            cte_1.state,
            cte_1.age,
            cte_1.provider,
            cte_1.coverage,
            cte_1.stability_score,
            cte_1.satisfaction_score,
            cte_1.claims_score,
            cte_1.coverage_score,
            cte_1.state_pop_wt_rate_mandatory + cte_1.state_pop_wt_rate_bi_pd + cte_1.state_pop_wt_rate_comp_coll + cte_1.state_pop_wt_rate_umbi + cte_1.state_pop_wt_rate_fees AS annual_premium,
            (cte_1.state_pop_wt_rate_mandatory + cte_1.state_pop_wt_rate_bi_pd + cte_1.state_pop_wt_rate_comp_coll + cte_1.state_pop_wt_rate_umbi + cte_1.state_pop_wt_rate_fees) / 12::numeric AS monthly_premium
           FROM cte_1
        ), cte_min AS (
         SELECT cte_2.data_date,
            cte_2.state,
            cte_2.age,
            cte_2.coverage,
            min(cte_2.annual_premium) AS min_premium,
            max(cte_2.annual_premium) AS max_premium
           FROM cte_2
          GROUP BY cte_2.data_date, cte_2.state, cte_2.age, cte_2.coverage
        ), cte_3 AS (
         SELECT a.data_date,
            a.state,
            a.age,
            a.provider,
            a.coverage,
            a.stability_score,
            a.satisfaction_score,
            a.claims_score,
            a.coverage_score,
            a.annual_premium,
            a.monthly_premium,
            m.min_premium,
            m.max_premium,
            (1::numeric - (a.annual_premium - m.min_premium) / (m.max_premium - m.min_premium)) * 5::numeric AS affordability_score
           FROM cte_2 a
             LEFT JOIN cte_min m ON a.data_date = m.data_date AND a.state::text = m.state::text AND a.age::text = m.age::text AND a.coverage = m.coverage
        ), cte_score AS (
         SELECT a.data_date,
            a.state,
            a.age,
            a.provider,
            a.coverage,
            a.stability_score,
            a.satisfaction_score,
            a.claims_score,
            a.coverage_score,
            a.affordability_score,
            a.annual_premium,
            a.monthly_premium,
			/**
			0.05 * a.stability_score * 20 as stability_score_100,
            0.35 * a.satisfaction_score * 20 as satisfaction_score_100,
            0.20 * a.claims_score * 20 as claims_score_100,
            0.10 * a.coverage_score * 20 as coverage_score_100,
            0.30 * a.affordability_score * 20 as affordability_score_100,
			**/
            (0.05 * a.stability_score + 0.35 * a.satisfaction_score + 0.20 * a.claims_score + 0.10 * a.coverage_score + 0.30 * a.affordability_score) * 20::numeric AS mg_total_best_100
           FROM cte_3 a
        ),cte_scores as (
			select 
				state ,
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
			from cte_score
			group by state
		), cte_best AS (
         SELECT a.data_date,
            a.state,
            a.age,
            a.provider,
            a.coverage,
            a.stability_score,
            a.satisfaction_score,
            a.claims_score,
            a.coverage_score,
            a.affordability_score,
            a.annual_premium,
            a.monthly_premium,
            a.mg_total_best_100,
            dense_rank() OVER (PARTITION BY a.data_date, a.state, a.age, a.coverage ORDER BY a.mg_total_best_100 DESC) AS rank_state_best
           FROM cte_score a
          WHERE a.mg_total_best_100 IS NOT NULL
        UNION
         SELECT a.data_date,
            a.state,
            a.age,
            a.provider,
            a.coverage,
            a.stability_score,
            a.satisfaction_score,
            a.claims_score,
            a.coverage_score,
            a.affordability_score,
            a.annual_premium,
            a.monthly_premium,
            a.mg_total_best_100,
            NULL::bigint AS rank_state_best
           FROM cte_score a
          WHERE a.mg_total_best_100 IS NULL
        ),
		cte_final as (
		 SELECT cte_best.data_date,
			cte_best.state,
			cte_best.age,
			cte_best.provider,
			cte_best.coverage,
			case when cte_best.stability_score is null then 'No Score' 
			else round(cte_best.stability_score,2) :: text end as stability_score ,
			case when cte_best.satisfaction_score is null then 'No Score'
			else round(cte_best.satisfaction_score,2) :: text end as satisfaction_score ,
			case when cte_best.claims_score is null then 'No Score'
			else round(cte_best.claims_score,2) :: text end as claims_score,  
			case when cte_best.coverage_score is null then 'No Score'
			else round(cte_best.coverage_score,2) :: text  end as coverage_score,
			cte_best.affordability_score,
			cte_best.annual_premium,
			cte_best.monthly_premium,
			case when (max_stability_score-min_stability_score) = 0 then 5
			else 
			((stability_score - cte_scores.min_stability_score)/(cte_scores.max_stability_score - cte_scores.min_stability_score) * (5-3))+3 end as stability_score_05,
			case when (max_satisfaction_score-min_satisfaction_score) = 0 then 35
			else 
			((satisfaction_score - cte_scores.min_satisfaction_score)/(cte_scores.max_satisfaction_score - cte_scores.min_satisfaction_score) * (35-28))+28 end as satisfaction_score_35,
			case when (max_claims_score-min_claims_score) = 0 then 20
			else 
			((claims_score - cte_scores.min_claims_score)/(cte_scores.max_claims_score - cte_scores.min_claims_score) * (20-8))+8 end as claims_score_20,
			case when (max_coverage_score-min_coverage_score) = 0 then 10
			else 
			((coverage_score - cte_scores.min_coverage_score)/(cte_scores.max_coverage_score - cte_scores.min_coverage_score) * (10-5))+5 end as coverage_score_10,
			case when (max_affordability_score-min_affordability_score) = 0 then 30
			else 
			((affordability_score - cte_scores.min_affordability_score)/(cte_scores.max_affordability_score - cte_scores.min_affordability_score) * (30-10))+10 end as affordability_score_30
		   FROM cte_best
		   left join cte_scores on cte_best.state = cte_scores.state
   ),
	   cte_rankings as (
			select *,
			dense_rank() OVER (PARTITION BY data_date, state, age,coverage ORDER BY stability_score_05 DESC NULLS LAST)::varchar AS stability_rank,
			dense_rank() OVER (PARTITION BY data_date, state, age,coverage ORDER BY satisfaction_score_35 DESC NULLS LAST)::varchar AS satisfaction_rank,
			dense_rank() OVER (PARTITION BY data_date, state, age,coverage ORDER BY claims_score_20 DESC NULLS LAST)::varchar AS claims_rank,
			dense_rank() OVER (PARTITION BY data_date, state, age,coverage ORDER BY coverage_score_10 DESC NULLS LAST)::varchar AS coverage_rank,
			dense_rank() OVER (PARTITION BY data_date, state, age,coverage ORDER BY affordability_score_30 DESC NULLS LAST)::varchar AS affordability_rank,
			round((stability_score_05 +  satisfaction_score_35 + claims_score_20 +coverage_score_10 + affordability_score_30),2)::varchar as mg_total_best_100_normalized ,
			dense_rank() OVER (PARTITION BY data_date, state, age, coverage ORDER BY stability_score_05 + 
							  satisfaction_score_35 + claims_score_20 +coverage_score_10 + affordability_score_30 DESC NULLS LAST)::varchar AS rank_state_best		   
	   from cte_final	   
   ) select data_date,
   			state,
			age,
			provider,
			coverage,
			stability_score,
            satisfaction_score,
            claims_score,
            coverage_score,
            affordability_score,
			annual_premium,
			monthly_premium,
			stability_score_05,
			satisfaction_score_35,
			claims_score_20,
			coverage_score_10,
			affordability_score_30,
			case when mg_total_best_100_normalized is null then 'No Score'
			else mg_total_best_100_normalized end as mg_total_best_100_normalized,
			case when mg_total_best_100_normalized is null then 'No Score'
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
   from cte_rankings