-- Weekly additive metrics must equal the sum of their daily components.
WITH daily_rollup AS (
    SELECT
        DATE_TRUNC(date, WEEK(MONDAY)) AS week_start,
        ROUND(SUM(gross_revenue), 2)   AS gross_revenue,
        SUM(total_orders)              AS total_orders,
        SUM(sessions)                  AS sessions
    FROM {{ ref('mart_daily_metrics') }}
    GROUP BY week_start
)
SELECT w.week_start
FROM {{ ref('mart_weekly_metrics') }} w
JOIN daily_rollup d ON w.week_start = d.week_start
WHERE ABS(w.gross_revenue - d.gross_revenue) > 0.01
   OR w.total_orders != d.total_orders
   OR w.sessions     != d.sessions
