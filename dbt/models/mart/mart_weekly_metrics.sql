/*
  mart_weekly_metrics.sql
  =======================
  Weekly aggregated metrics for Google Sheets Tab 2.
  One row per week.

  Week definition:
  ────────────────
  ISO 8601: Monday to Sunday.
  week_start = Monday of the week.
  week_end   = Sunday of the week.

  Note: GA4 uses Sunday-Saturday weeks internally.
  We use Monday-Sunday (ISO 8601) which is standard
  for business reporting. This is documented here
  for reconciliation purposes.

  Weekly totals reconcile with daily data:
  SUM(daily gross_revenue) WHERE date BETWEEN week_start AND week_end
  = mart_weekly_metrics.gross_revenue for that week.
*/

{{
    config(
        materialized='table'
    )
}}

WITH daily AS (
    SELECT * FROM {{ ref('mart_daily_metrics') }}
)

SELECT
    -- Week boundaries (ISO 8601 Monday-Sunday)
    DATE_TRUNC(date, WEEK(MONDAY))                              AS week_start,
    DATE_ADD(DATE_TRUNC(date, WEEK(MONDAY)), INTERVAL 6 DAY)    AS week_end,

    -- Revenue
    ROUND(SUM(gross_revenue), 2)                                AS gross_revenue,
    ROUND(SUM(refund_amount), 2)                                AS refund_amount,
    ROUND(SUM(net_revenue), 2)                                  AS net_revenue,

    -- Orders
    SUM(total_orders)                                           AS total_orders,
    ROUND(
        SUM(gross_revenue) / NULLIF(SUM(total_orders), 0)
    , 2)                                                        AS avg_order_value,

    -- Customers
    -- Note: unique_customers weekly != SUM(daily unique_customers)
    -- because same customer can buy on multiple days in a week.
    -- We use MAX here as an approximation — production would use
    -- COUNT DISTINCT from source for exact weekly uniques.
    SUM(new_customers)                                          AS new_customers,
    SUM(returning_customers)                                    AS returning_customers,

    -- Traffic
    SUM(sessions)                                               AS sessions,
    ROUND(
        SUM(total_orders) / NULLIF(SUM(sessions), 0)
    , 4)                                                        AS conversion_rate

FROM daily
GROUP BY week_start, week_end
ORDER BY week_start ASC
