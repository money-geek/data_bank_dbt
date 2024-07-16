{{ config(
    materialized='incremental',
) }}

select *
from {{ ref('ins_auto_pop_wt_new_premiums') }} t1
{% if is_incremental() %}
where
    (t1.data_date) not in (
        select distinct
            data_date
        from {{ this }}
    )

{% endif %}


--select distinct data_date from {{ ref('ins_auto_pop_wt_new_premiums') }} order by data_date