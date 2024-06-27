{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key=['customer_id','country','channel','typology','acquisition_month']
) }}

WITH customer_financials AS (
    SELECT
        customer_id,
        channel,
        device_id,
        ltv/cac as ltv_cac_ratio,
        ltv, 
        cac
    FROM {{ ref('financials') }}
),
customer_features AS (
    SELECT
        customer_id,
        country,
        typology
    FROM {{ ref('store') }}
    UNION
    SELECT
        customer_id,
        country,
        typology
    FROM {{ ref('funnel') }}
),
customer_acq_month AS (
    SELECT
        customer_id,
        min(acquisition_month) as acquisition_month
    FROM {{ ref('sales') }}
    GROUP BY customer_id
)
SELECT
    customer_financials.customer_id,
    customer_features.country,
    customer_financials.channel,
    customer_features.typology,
    customer_acq_month.acquisition_month,
    sum(customer_financials.ltv) as total_ltv,
    sum(customer_financials.cac) as total_cac,
    avg(customer_financials.cac) as avg_cac,
    sum(customer_financials.ltv)/sum(customer_financials.cac) as ltv_ratio,
    '{{ run_started_at.strftime("%Y-%m-%d") }}' as dw_created_at
FROM customer_financials
LEFT JOIN customer_features on customer_features.customer_id = customer_financials.customer_id
LEFT JOIN customer_acq_month on customer_acq_month.customer_id = customer_financials.customer_id
GROUP BY customer_financials.customer_id, country,channel, typology,acquisition_month

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where dw_created_at > current_date - interval '7 day'
{% endif %}