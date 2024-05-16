{{ config(materialized='table') }}


with cte as (
SELECT ins_auto_monthly_new_premiums.national_provider,
        count(DISTINCT ins_auto_monthly_new_premiums.state) AS state_presence
        FROM {{ ref('ins_auto_monthly_new_premiums') }} ins_auto_monthly_new_premiums
        GROUP BY ins_auto_monthly_new_premiums.national_provider)
select national_provider,state_presence,
CASE WHEN state_presence >= 29 THEN 'Yes'::TEXT ELSE 'No'::TEXT END AS national_presence
from cte