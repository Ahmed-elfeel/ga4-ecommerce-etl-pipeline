-- Net revenue must equal gross minus refunds (allowing for rounding).
SELECT date, gross_revenue, refund_amount, net_revenue
FROM {{ ref('mart_daily_metrics') }}
WHERE ABS(net_revenue - (gross_revenue - refund_amount)) > 0.01
