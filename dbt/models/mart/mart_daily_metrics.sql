/*
  mart_daily_metrics.sql
  ======================
  Daily aggregated metrics for Google Sheets Tab 1.
  One row per day.

  Metrics:
  ────────
  Revenue:   gross_revenue, refund_amount, net_revenue
  Orders:    total_orders, avg_order_value
  Customers: unique_customers, new_customers, returning_customers
  Traffic:   sessions, conversion_rate

  Definitions:
  ────────────
  - gross_revenue:      SUM of purchase_revenue_in_usd (transaction level)
  - refund_amount:      SUM of refund_value_in_usd
  - net_revenue:        gross_revenue - refund_amount
  - total_orders:       COUNT of distinct transactions
  - avg_order_value:    gross_revenue / total_orders
  - unique_customers:   COUNT DISTINCT users who purchased
  - new_customers:      users whose first purchase is on this date
  - returning_customers: unique_customers - new_customers
  - sessions:           COUNT DISTINCT sessions from stg_sessions
  - conversion_rate:    total_orders / sessions
*/

{{
    config(
        materialized='table'
    )
}}

WITH daily_purchases AS (
    SELECT
        p.purchase_date                                         AS date,
        ROUND(SUM(p.revenue_usd), 2)                           AS gross_revenue,
        ROUND(SUM(COALESCE(p.refund_usd, 0)), 2)               AS refund_amount,
        ROUND(
            SUM(p.revenue_usd) - SUM(COALESCE(p.refund_usd, 0))
        , 2)                                                    AS net_revenue,
        -- An order = one purchase event. transaction_id is NOT unique per order
        -- in this obfuscated dataset (distinct purchases collapse onto shared
        -- ids), so event_id (one row per purchase event) is the reliable grain.
        COUNT(DISTINCT p.event_id)                              AS total_orders,
        ROUND(
            SUM(p.revenue_usd) / NULLIF(COUNT(DISTINCT p.event_id), 0)
        , 2)                                                    AS avg_order_value,
        COUNT(DISTINCT p.user_pseudo_id)                        AS unique_customers,

        -- Clean new customer calculation using stg_users.first_purchase_date
        -- New customer = this is their first ever purchase in the dataset
        COUNT(DISTINCT CASE
            WHEN p.purchase_date = u.first_purchase_date
            THEN p.user_pseudo_id
        END)                                                    AS new_customers

    FROM {{ ref('stg_purchases') }} p
    LEFT JOIN {{ ref('stg_users') }} u
        ON p.user_pseudo_id = u.user_pseudo_id
    GROUP BY p.purchase_date
),

daily_sessions AS (
    SELECT
        session_date                                            AS date,
        COUNT(DISTINCT session_key)                             AS sessions
    FROM {{ ref('stg_sessions') }}
    GROUP BY session_date
)

SELECT
    COALESCE(p.date, s.date)                                    AS date,

    -- Revenue
    COALESCE(p.gross_revenue, 0)                                AS gross_revenue,
    COALESCE(p.refund_amount, 0)                                AS refund_amount,
    COALESCE(p.net_revenue, 0)                                  AS net_revenue,

    -- Orders
    COALESCE(p.total_orders, 0)                                 AS total_orders,
    COALESCE(p.avg_order_value, 0)                              AS avg_order_value,

    -- Customers
    COALESCE(p.unique_customers, 0)                             AS unique_customers,
    COALESCE(p.new_customers, 0)                                AS new_customers,
    COALESCE(p.unique_customers, 0)
        - COALESCE(p.new_customers, 0)                          AS returning_customers,

    -- Traffic
    COALESCE(s.sessions, 0)                                     AS sessions,

    -- Conversion rate: orders / sessions
    ROUND(
        COALESCE(p.total_orders, 0)
        / NULLIF(COALESCE(s.sessions, 0), 0)
    , 4)                                                        AS conversion_rate

FROM daily_purchases p
FULL OUTER JOIN daily_sessions s ON p.date = s.date
ORDER BY date ASC
