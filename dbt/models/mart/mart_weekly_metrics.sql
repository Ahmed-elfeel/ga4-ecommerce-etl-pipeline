/*
  mart_weekly_metrics.sql
  =======================
  Weekly aggregated metrics for Google Sheets Tab 2.
  One row per week (ISO 8601: Monday–Sunday).

  Additive metrics (revenue, orders, sessions) roll up from daily — weekly
  totals reconcile to the daily tab by construction.

  Non-additive metrics (unique_customers, returning_customers) are recomputed
  at week grain from source. A customer active on multiple days in the same
  week is counted once. Summing daily unique_customers would overcount.

  Week definition:
  ────────────────
  ISO 8601: Monday to Sunday.
  week_start = Monday of the week.
  week_end   = Sunday of the week.
*/

{{
    config(
        materialized='table'
    )
}}

WITH daily AS (
    SELECT * FROM {{ ref('mart_daily_metrics') }}
),

-- Additive metrics roll up cleanly from daily → reconcile by construction.
-- new_customers IS additive: a user's first-ever purchase falls on exactly one
-- day, so summing daily new_customers across a week = distinct new that week.
weekly_additive AS (
    SELECT
        DATE_TRUNC(date, WEEK(MONDAY))                          AS week_start,
        DATE_ADD(DATE_TRUNC(date, WEEK(MONDAY)), INTERVAL 6 DAY) AS week_end,
        ROUND(SUM(gross_revenue), 2)                            AS gross_revenue,
        ROUND(SUM(refund_amount), 2)                            AS refund_amount,
        ROUND(SUM(net_revenue), 2)                              AS net_revenue,
        SUM(total_orders)                                       AS total_orders,
        SUM(sessions)                                           AS sessions,
        SUM(new_customers)                                      AS new_customers
    FROM daily
    GROUP BY week_start, week_end
),

-- Non-additive: a customer active on multiple days in a week is counted ONCE.
-- Recomputed from source at week grain (cannot be summed from daily).
weekly_customers AS (
    SELECT
        DATE_TRUNC(purchase_date, WEEK(MONDAY))                 AS week_start,
        COUNT(DISTINCT user_pseudo_id)                          AS unique_customers
    FROM {{ ref('stg_purchases') }}
    GROUP BY week_start
)

SELECT
    a.week_start,
    a.week_end,

    -- Revenue
    a.gross_revenue,
    a.refund_amount,
    a.net_revenue,

    -- Orders
    a.total_orders,
    ROUND(a.gross_revenue / NULLIF(a.total_orders, 0), 2)       AS avg_order_value,

    -- Customers (unique recomputed at week grain; returning derived)
    COALESCE(c.unique_customers, 0)                             AS unique_customers,
    a.new_customers,
    COALESCE(c.unique_customers, 0) - a.new_customers           AS returning_customers,

    -- Traffic
    a.sessions,
    ROUND(a.total_orders / NULLIF(a.sessions, 0), 4)            AS conversion_rate

FROM weekly_additive a
LEFT JOIN weekly_customers c ON a.week_start = c.week_start
ORDER BY a.week_start ASC
