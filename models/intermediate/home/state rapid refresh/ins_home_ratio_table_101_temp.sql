{{ config(materialized='table') }}
SELECT  zipcode,
		t1.provider,
	    t1.city,
	    t1.state_name,
	    t1.state,
		t1.cov_a_dwelling,
		t1.place_tier,
		t1.credit_tier,
		t1.claims_history,
		t1.protection_class,
	   	t1.construction_type,
		t1.roof_type,
		t1.all_perils_deductible,
		t1.annualpremium as old_annualpremium,
		coalesce(t1.place_tier_rank,t3.place_tier_rank_1,t4.place_tier_rank_1 ) as place_tier_rank,
		t1.coverages,t1.construction_year,
		coalesce (t1.annualpremium * t2.rate, t1.annualpremium * t3.rate, t1.annualpremium * t4.rate,t1.annualpremium) as annualpremium
		 FROM(
			select * from ins_home_report_master_table_2023 
			where not (state = 'NC' and provider in ('Nationwide','State Farm')) 
		 ) t1 
		 left join ins_home_ratio_table_108_temp t2
		 		ON t1.provider = t2.provider AND t1.state = t2.state and t1.place_tier_rank = t2.place_tier_rank and t1.city = t2.city 
		 left join (select *,'5'as place_tier_rank_1 from ins_home_ratio_table_108_temp
					where place_tier_rank = '4' ) t3
		 ON t1.provider = t3.provider AND t1.state = t3.state and t1.place_tier_rank = t3.place_tier_rank_1  and t1.city = t3.city 
		 left join (select *,'6'as place_tier_rank_1 from ins_home_ratio_table_108_temp
					where place_tier_rank = '4' ) t4
		 ON t1.provider = t4.provider AND t1.state = t4.state and t1.place_tier_rank = t4.place_tier_rank_1  and t1.city = t4.city 
		 WHERE report = 'report_101' 
		and t1.place_tier_rank is not null