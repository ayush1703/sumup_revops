{{ config(
    materialized='incremental',
    schema='intermediate',
    unique_key=['customer_id','sales_email','created_at']
) }}

WITH sales_funnel as (
SELECT
        sales_email,
        funnel.typology,
        funnel.country,
        funnel.customer_id,
        created_at,
        live_date,
        created_at::date - live_date as date_difference,
        live_date - lead_creation_date as lead_live_time,
        live_date - qualified_date as qualify_live_time,
        qualified_date - lead_creation_date as lead_qualify_time,
        order_paid_date - live_date as live_order_paid_time,
    FROM {{ ref('funnel') }} as funnel
    LEFT JOIN {{ ref('store') }} as store on store.customer_id = funnel.customer_id
)
SELECT  
    sales_funnel.*,
    '{{ run_started_at.strftime("%Y-%m-%d") }}' as dw_created_at
FROM sales_funnel

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where dw_created_at > current_date - interval '7 day'
{% endif %}