{{ config(
    materialized='incremental',
) }}

select *
from {{ ref('ins_auto_monthly_new_premiums_city') }} t1
{% if is_incremental() %}
where
    (t1.data_date) not in (
        select distinct
            data_date
        from {{ this }}
    )

{% endif %}