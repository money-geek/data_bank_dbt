@echo off
setlocal enabledelayedexpansion

rem List of months
set months=2022-05-01 2022-06-01 2022-07-01 2022-08-01 2022-09-01 2022-10-01 2022-11-01 2022-12-01

rem Iterate over each month and run the DBT command
for %%m in (%months%) do (
    echo Running DBT for month: %%m
    dbt run -m ins_auto_monthly_change ins_auto_monthly_new_premiums ins_auto_pop_wt_new_premiums ins_auto_pop_wt_new_premiums_full_inserted --vars "{\"current_month\": \"%%m\"}"
)

endlocal