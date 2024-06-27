{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key=['channel','sales_email','typology']
) }}

WITH customer_financials AS (
    SELECT
        customer_id,
        channel,
        sum(ltv) as total_ltv, 
        sum(cac) as total_cac
    FROM {{ ref('financials') }}
    GROUP BY customer_id,channel
),
sales_rep AS(
    SELECT
        sales_email,
        typology,
        country,
        customer_id,
        live_date - lead_creation_date as lead_live_cycle
    FROM {{ ref('funnel') }}
)
SELECT
    sales_rep.sales_email,
    sales_rep.typology,
    sales_rep.country,
    customer_financials.channel,
    count(distinct sales_rep.customer_id) as customer_count,
    avg(sales_rep.lead_live_cycle) as avg_lead_to_live_time,
    sum(customer_financials.total_ltv) as total_ltv,
    sum(customer_financials.total_ltv)/count(distinct sales_rep.customer_id) as avg_ltv_per_customer,
    sum(customer_financials.total_cac) as total_cac,
    sum(customer_financials.total_ltv)/sum(customer_financials.total_cac) as ltv_ratio,
    '{{ run_started_at.strftime("%Y-%m-%d") }}' as dw_created_at
FROM sales_rep
LEFT JOIN customer_financials on sales_rep.customer_id = customer_financials.customer_id
GROUP BY sales_rep.sales_email, sales_rep.typology, sales_rep.country, customer_financials.channel

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where dw_created_at > current_date - interval '7 day'
{% endif %}