
{{ config(materialized='table') }}

SELECT c.data_date ,
    c.national_presence,
    c.age,
      CASE
            WHEN c.age::text = '25'::text THEN '22-29'::text
            WHEN c.age::text = '40'::text THEN '30-59'::text
            WHEN c.age::text = '65'::text THEN '60 +'::text
            ELSE NULL::text
        END AS age_range,
    c.national_provider,
    c.coverage,
 CASE
    WHEN percent_rank() OVER (PARTITION BY c.data_date, c.age, c.coverage ORDER BY c.annual_premium) <= 0.33 THEN 'cheap'
    WHEN percent_rank() OVER (PARTITION BY c.data_date, c.age, c.coverage ORDER BY c.annual_premium) <= 0.66 THEN 'moderate'
    ELSE 'expensive'
  END AS cost_bucket,
    case when coverage = 'State Min - 1000 comp_coll' then  'State Minimum Liability w/ Full Cov. w/$1,000 Ded.'
         when coverage = 'State Min - No comp_coll' then  'State Minimum Liability Only'
         when coverage = '100/300/100,000 - 1000 comp_coll' then  '100/300/100 Full Cov. w/$1,000 Ded.'
         when coverage = '100/300/100,000 - No comp_coll' then  '100/300/100 Liability Only'
         when coverage = '300/500/300,000 - 1000 comp_coll' then  '300/500/300 Full Cov. w/$1,000 Ded'
         when coverage = '300/500/300,000 - No comp_coll' then  '300/500/300 Liability Only'
         when coverage = '100/300/100,000 - 500 comp_coll' then '100/300/100 Full Cov. w/$500 Ded.'
         when coverage = 'State Min - 500 comp_coll' then 'State Minimum Liability w/ Full Cov. w/$500 Ded.'
         when coverage = 'State Min - 1500 comp_coll' then 'State Minimum Liability w/ Full Cov. w/$1,500 Ded.'
         when coverage = '100/300/100,000 - 1500 comp_coll' then '100/300/100 Full Cov. w/$1,500 Ded.'
         when coverage =  '300/500/300,000 - 1500 comp_coll' then  '300/500/300 Full Cov. w/$1,500 Ded.'
         when coverage = '300/500/300,000 - 500 comp_coll' then  '300/500/300 Full Cov. w/$500 Ded.'
else coverage end as coverage_user_friendly,
    c.stability_score as stability_score_05,
	c.satisfaction_score as satisfaction_score_35,
	c.claims_score as claims_score_20,
	c.coverage_score as coverage_score_10,
	c.affordability_score as affordability_score_30,
    c.annual_premium,
    c.monthly_premium,
    c.mg_total_best_100,
    c.rank_state_best,
    c.mg_best_rank_national_presence,
    dense_rank() OVER (PARTITION BY c.data_date, c.age, c.coverage ORDER BY c.annual_premium) AS cheapest_rank,
        CASE
            WHEN c.national_presence = 'No'::text THEN NULL::bigint
            ELSE dense_rank() OVER (PARTITION BY c.data_date, c.age, c.coverage, c.national_presence ORDER BY c.annual_premium)
        END AS cheapest_rank_national_presence,
   c.mg_total_best_100_normalized
   FROM ( WITH cte_1 AS (
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t2.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '100/300/100,000 - 1500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
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
                       FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }}  ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.comp_coll_deductible = '1500/1500'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '100/300/100,000 - 1500 comp_coll'::text) t
                     left JOIN ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '100/300/100,000 - 1500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = '100/300/100000'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '100/300/100,000 - 1500 comp_coll'::text) t2 On  t.age::text = t2.age::text AND t.national_provider::text = t2.national_provider::text AND t.coverage = t2.coverage AND t.national_presence = t2.national_presence
                UNION
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t2.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '100/300/100,000 - 1000 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
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
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.comp_coll_deductible = '1000/1000'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '100/300/100,000 - 1000 comp_coll'::text) t
                     LEFT JOIN ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '100/300/100,000 - 1000 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = '100/300/100000'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '100/300/100,000 - 1000 comp_coll'::text) t2 ON t.data_date = t2.data_date AND t.age::text = t2.age::text AND t.national_provider::text = t2.national_provider::text AND t.coverage = t2.coverage AND t.national_presence = t2.national_presence
                UNION
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t2.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '100/300/100,000 - 500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
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
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.comp_coll_deductible = '500/500'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '100/300/100,000 - 500 comp_coll'::text) t
                     LEFT JOIN ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '100/300/100,000 - 500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = '100/300/100000'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '100/300/100,000 - 500 comp_coll'::text) t2 ON t.data_date = t2.data_date AND t.age::text = t2.age::text AND t.national_provider::text = t2.national_provider::text AND t.coverage = t2.coverage AND t.national_presence = t2.national_presence
                UNION
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t2.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            'State Min - 500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
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
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.comp_coll_deductible = '500/500'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, 'State Min - 500 comp_coll'::text) t
                     LEFT JOIN ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            'State Min - 500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = 'State Minimum'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, 'State Min - 500 comp_coll'::text) t2 ON t.data_date = t2.data_date AND t.age::text = t2.age::text AND t.national_provider::text = t2.national_provider::text AND t.coverage = t2.coverage AND t.national_presence = t2.national_presence
                UNION
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t2.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            'State Min - 1500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
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
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.comp_coll_deductible = '1500/1500'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, 'State Min - 1500 comp_coll'::text) t
                     LEFT JOIN ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            'State Min - 1500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = 'State Minimum'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, 'State Min - 1500 comp_coll'::text) t2 ON  t.age::text = t2.age::text AND t.national_provider::text = t2.national_provider::text AND t.coverage = t2.coverage AND t.national_presence = t2.national_presence
                UNION
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t2.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            'State Min - 1000 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
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
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.comp_coll_deductible = '1000/1000'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, 'State Min - 1000 comp_coll'::text) t
                     LEFT JOIN ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            'State Min - 1000 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = 'State Minimum'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, 'State Min - 1000 comp_coll'::text) t2 ON t.data_date = t2.data_date AND t.age::text = t2.age::text AND t.national_provider::text = t2.national_provider::text AND t.coverage = t2.coverage AND t.national_presence = t2.national_presence
                UNION
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t2.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '300/500/300,000 - 500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
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
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.comp_coll_deductible = '500/500'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '300/500/300,000 - 500 comp_coll'::text) t
                     LEFT JOIN ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '300/500/300,000 - 500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = '300/500/300000'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '300/500/300,000 - 500 comp_coll'::text) t2 ON  t.age::text = t2.age::text AND t.national_provider::text = t2.national_provider::text AND t.coverage = t2.coverage AND t.national_presence = t2.national_presence
                UNION
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t2.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '300/500/300,000 - 1000 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
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
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.comp_coll_deductible = '1000/1000'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '300/500/300,000 - 1000 comp_coll'::text) t
                     LEFT JOIN ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '300/500/300,000 - 1000 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = '300/500/300000'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '300/500/300,000 - 1000 comp_coll'::text) t2 ON  t.age::text = t2.age::text AND t.national_provider::text = t2.national_provider::text AND t.coverage = t2.coverage AND t.national_presence = t2.national_presence
                UNION
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t2.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '300/500/300,000 - 1500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
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
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.comp_coll_deductible = '1500/1500'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '300/500/300,000 - 1500 comp_coll'::text) t
                     LEFT JOIN ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '300/500/300,000 - 1500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = '300/500/300000'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '300/500/300,000 - 1500 comp_coll'::text) t2 ON  t.age::text = t2.age::text AND t.national_provider::text = t2.national_provider::text AND t.coverage = t2.coverage AND t.national_presence = t2.national_presence
                UNION
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '100/300/100,000 - No comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
                            0::numeric AS state_pop_wt_rate_comp_coll,
                            avg(
                                CASE
                                    WHEN ins_auto_master_table_2022.state::text <> ALL (ARRAY['Connecticut'::character varying::text, 'Illinois'::character varying::text, 'Kansas'::character varying::text, 'Maine'::character varying::text, 'Maryland'::character varying::text, 'Massachusetts'::character varying::text, 'Minnesota'::character varying::text, 'Missouri'::character varying::text, 'Nebraska'::character varying::text, 'New Hampshire'::character varying::text, 'New York'::character varying::text, 'North Carolina'::character varying::text, 'North Dakota'::character varying::text, 'Oregon'::character varying::text, 'South Carolina'::character varying::text, 'South Dakota'::character varying::text, 'Vermont'::character varying::text, 'Virginia'::character varying::text, 'District of Columbia'::character varying::text, 'West Virginia'::character varying::text, 'Wisconsin'::character varying::text]) THEN 0::numeric
                                    ELSE ins_auto_master_table_2022.state_pop_wt_rate_umbi
                                END) AS state_pop_wt_rate_umbi,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_fees) AS state_pop_wt_rate_fees,
                            avg(ins_auto_master_table_2022.stability_score) AS stability_score,
                            avg(ins_auto_master_table_2022.satisfaction_score) AS satisfaction_score,
                            avg(ins_auto_master_table_2022.claims_score) AS claims_score,
                            avg(ins_auto_master_table_2022.coverage_score) AS coverage_score,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = '100/300/100000'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '100/300/100,000 - No comp_coll'::text) t
                UNION
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            'State Min - No comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
                            0::numeric AS state_pop_wt_rate_comp_coll,
                            avg(
                                CASE
                                    WHEN ins_auto_master_table_2022.state::text <> ALL (ARRAY['Connecticut'::character varying::text, 'Illinois'::character varying::text, 'Kansas'::character varying::text, 'Maine'::character varying::text, 'Maryland'::character varying::text, 'Massachusetts'::character varying::text, 'Minnesota'::character varying::text, 'Missouri'::character varying::text, 'Nebraska'::character varying::text, 'New Hampshire'::character varying::text, 'New York'::character varying::text, 'North Carolina'::character varying::text, 'North Dakota'::character varying::text, 'Oregon'::character varying::text, 'South Carolina'::character varying::text, 'South Dakota'::character varying::text, 'Vermont'::character varying::text, 'Virginia'::character varying::text, 'District of Columbia'::character varying::text, 'West Virginia'::character varying::text, 'Wisconsin'::character varying::text]) THEN 0::numeric
                                    ELSE ins_auto_master_table_2022.state_pop_wt_rate_umbi
                                END) AS state_pop_wt_rate_umbi,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_fees) AS state_pop_wt_rate_fees,
                            avg(ins_auto_master_table_2022.stability_score) AS stability_score,
                            avg(ins_auto_master_table_2022.satisfaction_score) AS satisfaction_score,
                            avg(ins_auto_master_table_2022.claims_score) AS claims_score,
                            avg(ins_auto_master_table_2022.coverage_score) AS coverage_score,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = 'State Minimum'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, 'State Min - No comp_coll'::text) t
             UNION
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '300/500/300,000 - No comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
                            0::numeric AS state_pop_wt_rate_comp_coll,
                            avg(
                                CASE
                                    WHEN ins_auto_master_table_2022.state::text <> ALL (ARRAY['Connecticut'::character varying::text, 'Illinois'::character varying::text, 'Kansas'::character varying::text, 'Maine'::character varying::text, 'Maryland'::character varying::text, 'Massachusetts'::character varying::text, 'Minnesota'::character varying::text, 'Missouri'::character varying::text, 'Nebraska'::character varying::text, 'New Hampshire'::character varying::text, 'New York'::character varying::text, 'North Carolina'::character varying::text, 'North Dakota'::character varying::text, 'Oregon'::character varying::text, 'South Carolina'::character varying::text, 'South Dakota'::character varying::text, 'Vermont'::character varying::text, 'Virginia'::character varying::text, 'District of Columbia'::character varying::text, 'West Virginia'::character varying::text, 'Wisconsin'::character varying::text]) THEN 0::numeric
                                    ELSE ins_auto_master_table_2022.state_pop_wt_rate_umbi
                                END) AS state_pop_wt_rate_umbi,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_fees) AS state_pop_wt_rate_fees,
                            avg(ins_auto_master_table_2022.stability_score) AS stability_score,
                            avg(ins_auto_master_table_2022.satisfaction_score) AS satisfaction_score,
                            avg(ins_auto_master_table_2022.claims_score) AS claims_score,
                            avg(ins_auto_master_table_2022.coverage_score) AS coverage_score,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = '300/500/300000'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '300/500/300,000 - No comp_coll'::text) t
       
	   UNION
                 SELECT t.data_date,
                    t.age,
                    t.national_provider,
                    t.national_presence,
                    t.coverage,
                    t.state_pop_wt_rate_mandatory,
                    t.state_pop_wt_rate_comp_coll,
                    t.state_pop_wt_rate_umbi,
                    t.state_pop_wt_rate_fees,
                    t.stability_score,
                    t.satisfaction_score,
                    t.claims_score,
                    t.coverage_score,
                    t.state_pop_wt_rate_bi_pd
                   FROM ( SELECT  data_date,
                            ins_auto_master_table_2022.age,
                            ins_auto_master_table_2022.national_provider,
                            ins_auto_master_table_2022.national_presence,
                            '300/500/300,000 - 500 comp_coll'::text AS coverage,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_mandatory) AS state_pop_wt_rate_mandatory,
                            0::numeric AS state_pop_wt_rate_comp_coll,
                            avg(
                                CASE
                                    WHEN ins_auto_master_table_2022.state::text <> ALL (ARRAY['Connecticut'::character varying::text, 'Illinois'::character varying::text, 'Kansas'::character varying::text, 'Maine'::character varying::text, 'Maryland'::character varying::text, 'Massachusetts'::character varying::text, 'Minnesota'::character varying::text, 'Missouri'::character varying::text, 'Nebraska'::character varying::text, 'New Hampshire'::character varying::text, 'New York'::character varying::text, 'North Carolina'::character varying::text, 'North Dakota'::character varying::text, 'Oregon'::character varying::text, 'South Carolina'::character varying::text, 'South Dakota'::character varying::text, 'Vermont'::character varying::text, 'Virginia'::character varying::text, 'District of Columbia'::character varying::text, 'West Virginia'::character varying::text, 'Wisconsin'::character varying::text]) THEN 0::numeric
                                    ELSE ins_auto_master_table_2022.state_pop_wt_rate_umbi
                                END) AS state_pop_wt_rate_umbi,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_fees) AS state_pop_wt_rate_fees,
                            avg(ins_auto_master_table_2022.stability_score) AS stability_score,
                            avg(ins_auto_master_table_2022.satisfaction_score) AS satisfaction_score,
                            avg(ins_auto_master_table_2022.claims_score) AS claims_score,
                            avg(ins_auto_master_table_2022.coverage_score) AS coverage_score,
                            avg(ins_auto_master_table_2022.state_pop_wt_rate_bi_pd) AS state_pop_wt_rate_bi_pd
                           FROM {{ ref('ins_auto_pop_wt_new_premiums_full_inserted') }} ins_auto_master_table_2022
                          WHERE  (ins_auto_master_table_2022.age::text = ANY (ARRAY['25'::character varying::text, '40'::character varying::text, '65'::character varying::text])) AND ins_auto_master_table_2022.vehicles::text = '2012 Toyota Camry LE'::text AND (ins_auto_master_table_2022.insurancescore_alignment::text = ANY (ARRAY['Blank'::character varying::text, 'Good'::character varying::text])) AND ins_auto_master_table_2022.driving_record_violations::text = 'Clean'::text and is_user_a_veteran ='No'   AND ins_auto_master_table_2022.bi_pd_limit = '300/500/300000'::text
                          GROUP BY data_date, ins_auto_master_table_2022.age, ins_auto_master_table_2022.national_presence, ins_auto_master_table_2022.national_provider, '300/500/300,000 - 500 comp_coll'::text) t
                ), cte_2 AS (
                 SELECT cte_1.data_date,
                    cte_1.national_presence,
                    cte_1.age,
                    cte_1.national_provider,
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
                    cte_2.age,
                    cte_2.coverage,
                    min(cte_2.annual_premium) AS min_premium,
                    max(cte_2.annual_premium) AS max_premium
                   FROM cte_2
                  GROUP BY cte_2.data_date, cte_2.age, cte_2.coverage
                ), cte_3 AS (
                 SELECT a.data_date,
                    a.national_presence,
                    a.age,
                    a.national_provider,
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
                     LEFT JOIN cte_min m ON a.data_date = m.data_date AND a.age::text = m.age::text AND a.coverage = m.coverage
                ),cte_scores as (
					select age,coverage,data_date,
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
					group by age,coverage,data_date
				), cte_score AS (
                 SELECT a.data_date,
                    a.national_presence,
                    a.age,
                    a.national_provider,
                    a.coverage,
                    case when (max_stability_score-min_stability_score) = 0 then 5
					else 
					((stability_score - cte_scores.min_stability_score)/(cte_scores.max_stability_score - cte_scores.min_stability_score) * (5-1.6))+1.6 end as stability_score,
					case when (max_satisfaction_score-min_satisfaction_score) = 0 then 35
					else 
					((satisfaction_score - cte_scores.min_satisfaction_score)/(cte_scores.max_satisfaction_score - cte_scores.min_satisfaction_score) * (35-11.6))+11.6 end as satisfaction_score,
					case when (max_claims_score-min_claims_score) = 0 then 20
					else 
					((claims_score - cte_scores.min_claims_score)/(cte_scores.max_claims_score - cte_scores.min_claims_score) * (20-6.6))+6.6 end as claims_score,
					case when (max_coverage_score-min_coverage_score) = 0 then 10
					else 
					((coverage_score - cte_scores.min_coverage_score)/(cte_scores.max_coverage_score - cte_scores.min_coverage_score) * (10-3.3))+3.3 end as coverage_score,
					case when (max_affordability_score-min_affordability_score) = 0 then 30
					else 
					((affordability_score - cte_scores.min_affordability_score)/(cte_scores.max_affordability_score - cte_scores.min_affordability_score) * (30-10))+10 end as affordability_score,
                    a.annual_premium,
                    a.monthly_premium
					FROM cte_3 a
					left join cte_scores 
					on cte_scores.age = a.age and cte_scores.coverage = a.coverage and  cte_scores.data_date = a.data_date
                ), cte_best AS (
                 SELECT a.data_date,
                    a.national_presence,
                    a.age,
                    a.national_provider,
                    a.coverage,
                    a.stability_score,
                    a.satisfaction_score,
                    a.claims_score,
                    a.coverage_score,
                    a.affordability_score,
                    a.annual_premium,
                    a.monthly_premium,
                    round((stability_score +  satisfaction_score + claims_score +coverage_score + affordability_score),2) as mg_total_best_100,
                    dense_rank() OVER (PARTITION BY a.data_date, a.age, a.coverage ORDER BY (stability_score +  satisfaction_score + claims_score +coverage_score + affordability_score)DESC NULLS LAST)::varchar AS rank_state_best,
					round((stability_score +  satisfaction_score + claims_score +coverage_score + affordability_score),2) as mg_total_best_100_normalized,
					CASE WHEN a.national_presence = 'No'::text THEN NULL
            		ELSE dense_rank() OVER (PARTITION BY a.data_date, a.age, a.coverage, a.national_presence ORDER BY (stability_score +  satisfaction_score + claims_score +coverage_score + affordability_score) DESC NULLS LAST)::varchar
        			END AS mg_best_rank_national_presence
                   FROM cte_score a
                )
				   SELECT cte_best.data_date,
				   cte_best.national_presence,
				   cte_best.age,
				   cte_best.national_provider,
				   cte_best.coverage,
				   CASE WHEN cte_best.stability_score::varchar IS NULL THEN 'No Score' ELSE round(cte_best.stability_score,2)::varchar END AS stability_score,
				   CASE WHEN cte_best.satisfaction_score::varchar IS NULL THEN 'No Score' ELSE round(cte_best.satisfaction_score,2)::varchar END AS satisfaction_score,
				   CASE WHEN cte_best.claims_score::varchar IS NULL THEN 'No Score' ELSE round(cte_best.claims_score,2)::varchar END AS claims_score,
				   CASE WHEN cte_best.coverage_score::varchar IS NULL THEN 'No Score' ELSE round(cte_best.coverage_score,2)::varchar END AS coverage_score,
				   CASE WHEN cte_best.affordability_score::varchar IS NULL THEN 'No Score' ELSE round(cte_best.affordability_score,2)::varchar END AS affordability_score,
				   cte_best.annual_premium,
				   cte_best.monthly_premium,
				   CASE WHEN cte_best.mg_total_best_100::varchar IS NULL THEN 'No Score' ELSE cte_best.mg_total_best_100::varchar END AS mg_total_best_100,
				   CASE WHEN cte_best.mg_total_best_100_normalized::varchar IS NULL THEN 'No Score' ELSE cte_best.mg_total_best_100_normalized::varchar END AS mg_total_best_100_normalized,
				   CASE WHEN cte_best.mg_total_best_100_normalized::varchar IS NULL THEN 'No Score' ELSE cte_best.rank_state_best::varchar END AS rank_state_best,
				   CASE WHEN cte_best.mg_best_rank_national_presence::varchar IS NULL THEN 'No Score' ELSE cte_best.mg_best_rank_national_presence::varchar END AS mg_best_rank_national_presence
		 		   
			FROM cte_best) c
