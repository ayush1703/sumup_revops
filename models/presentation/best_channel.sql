{{ config(materialized='view') }}

SELECT
    country,
    channel,
    count(distinct customer_id) as customer_count,
    sum(total_ltv) as total_ltv
FROM {{ ref('customer_financials') }}
GROUP BY country, channel
ORDER BY total_ltv DESC