{{ config(
    materialized='incremental',
) }}

select 
    t1.data_date,
    t1.state,
    t1.age,
    t1.provider,
    t1.coverage,
    t1.avg_annual_premium,
    t1.avg_monthly_premium,
    t1.age_range,
    t1.cheapest_rank,
    t1.provider_id
from {{ ref('ins_auto_cheap_state_minimumandfull_agesplit_temp_1') }} t1
{% if is_incremental() %}
where
    (t1.data_date) not in (
        select 
            data_date
        from {{ this }}
    )

{% endif %}



