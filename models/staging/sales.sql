{{ config(materialized='incremental', schema='staging', unique_key=['customer_id','channel','acquisition_month'] ) }}


WITH import_csv AS (
        SELECT * FROM read_csv_auto('../sales.csv')
    )
SELECT
    customer_id,
    channel,
    acquisition_month,
    '{{ run_started_at.strftime("%Y-%m-%d") }}' as dw_created_at
FROM import_csv
WHERE customer_id is not null

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where dw_created_at > current_date - interval '7 day'
{% endif %}