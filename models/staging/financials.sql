{{ config(materialized='incremental', schema='staging', unique_key=['customer_id','device_id']) }}


WITH import_csv AS (
        SELECT 
            *
        FROM read_csv_auto('../Financials.csv')
        WHERE id is not null AND device_id is not null 
    )
SELECT
    id as customer_id,
    device_id,
    "5_year_ltv" as ltv,
    cac,
    channel,
    '{{ run_started_at.strftime("%Y-%m-%d") }}' as dw_created_at
FROM import_csv

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where dw_created_at > current_date - interval '7 day'
{% endif %}