{{ config(
    materialized='incremental',
) }}

select *
from {{ ref('ins_auto_bestcheap_city_baseline_temp_1') }} t1
{% if is_incremental() %}
where
    (t1.data_date) not in (
        select distinct
            data_date
        from {{ this }}
    )

{% endif %}