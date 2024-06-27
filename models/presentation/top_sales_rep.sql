{{ config(materialized='view') }}

SELECT
    country,
    sales_email,
    customer_count,
    total_ltv,
    avg_lead_to_live_time
FROM {{ ref('sales_reps_performance') }}
ORDER BY total_ltv DESC