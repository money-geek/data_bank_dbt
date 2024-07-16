@echo off
setlocal enabledelayedexpansion

rem List of months
set months=2024-01-01 2024-02-01 2024-03-01

rem Iterate over each month and run the DBT command
for %%m in (%months%) do (
    echo Running DBT for month: %%m
    dbt run -m ins_auto_monthly_change ins_auto_monthly_new_premiums ins_auto_pop_wt_new_premiums ins_auto_pop_wt_new_premiums_full_inserted --vars "{\"current_month\": \"%%m\"}"
)

endlocal