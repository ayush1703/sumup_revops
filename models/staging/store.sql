{{ config(materialized='incremental', schema='staging' , unique_key=['customer_id','created_at']) }}


WITH import_csv AS (        --CTE to load CSV data from store.csv
        SELECT * FROM read_csv_auto('../store.csv', normalize_names=True)
    )
SELECT
    customer_id,
    country,
    strptime(created_at, '%m/%d/%Y %I:%M:%S') as created_at,        --added timestamp format to set column in data model as datetime
    typology,
    '{{ run_started_at.strftime("%Y-%m-%d") }}' as dw_created_at
FROM import_csv
WHERE customer_id is not null and try_strptime(created_at, '%m/%d/%Y %I:%M:%S') is not null --to exclude other bad format timestamps

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where dw_created_at > current_date - interval '7 day'
{% endif %}