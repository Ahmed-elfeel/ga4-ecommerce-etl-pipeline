-- Sanity check that directly catches the transaction_id-collapse bug:
-- a day cannot have more new customers than orders.
SELECT date, total_orders, new_customers
FROM {{ ref('mart_daily_metrics') }}
WHERE new_customers > total_orders
