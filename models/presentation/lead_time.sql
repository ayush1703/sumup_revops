{{ config(materialized='view') }}

SELECT
    typology,
    AVG(lead_live_time) as avg_lead_live_time
FROM {{ ref('funnel_performance') }}
GROUP BY typology
ORDER BY avg_lead_live_time