{{ config(
    materialized='incremental',
) }}

select *
from {{ ref('ins_home_master_table_extrapolated_data_state_2024_temp') }} t1
{% if is_incremental() %}
where
    (t1.ratedate) not in (
        select distinct
            ratedate
        from {{ this }}
    )

{% endif %}