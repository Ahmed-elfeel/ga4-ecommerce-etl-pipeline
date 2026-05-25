/*
  mart_product_metrics.sql
  ========================
  Product-level aggregated metrics.
  One row per product (item_name).

  Metrics:
  ────────
  - total_revenue:    SUM of net item revenue
  - total_quantity:   SUM of units sold
  - total_orders:     COUNT of distinct transactions containing this product
  - avg_price:        AVG unit price in USD
  - total_refunds:    SUM of refund amounts

  Used for:
  - Top products by revenue
  - Top products by quantity sold
*/

{{
    config(
        materialized='table'
    )
}}

SELECT
    item_name,
    item_id,

    -- Category (most common category for this product)
    MAX(category_l1)                                            AS category,
    MAX(category_l2)                                            AS sub_category,

    -- Revenue metrics
    ROUND(SUM(item_revenue_usd), 2)                             AS total_revenue,
    ROUND(SUM(item_refund_usd), 2)                              AS total_refunds,
    ROUND(SUM(net_revenue_usd), 2)                              AS net_revenue,

    -- Volume metrics
    SUM(quantity)                                               AS total_quantity_sold,
    COUNT(DISTINCT transaction_id)                              AS total_orders,

    -- Pricing
    ROUND(AVG(price_in_usd), 2)                                 AS avg_price_usd,

    -- Rank by revenue and quantity (for top products reporting)
    RANK() OVER (ORDER BY SUM(item_revenue_usd) DESC)           AS revenue_rank,
    RANK() OVER (ORDER BY SUM(quantity) DESC)                   AS quantity_rank

FROM {{ ref('stg_purchase_items') }}
WHERE item_name IS NOT NULL
GROUP BY item_name, item_id
ORDER BY total_revenue DESC
