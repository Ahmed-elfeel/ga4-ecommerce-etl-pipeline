/*
  stg_purchase_items.sql
  ======================
  One row per item per transaction.
  Reads directly from raw_purchase_items (already unnested at raw layer).

  Key decisions:
  - item_event_id as unique_key
  - event_id kept for joining to stg_purchases
  - Minimal transformation — raw_purchase_items already clean
*/

{{
    config(
        materialized='incremental',
        unique_key='item_event_id',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    -- Primary key
    item_event_id,

    -- Foreign keys
    event_id,
    transaction_id,

    event_date_dt                                   AS purchase_date,

    -- User
    user_pseudo_id,

    -- Item position within order
    item_position,

    -- Item identifiers
    item_id,
    item_name,
    item_brand,
    item_variant,

    -- Category hierarchy
    item_category                                   AS category_l1,
    item_category2                                  AS category_l2,
    item_category3                                  AS category_l3,

    -- Pricing
    price_in_usd,
    quantity,
    ROUND(price_in_usd * quantity, 2)               AS gross_revenue_usd,

    -- Actual revenue (may differ from price * qty due to discounts)
    COALESCE(item_revenue_in_usd, 0)                AS item_revenue_usd,
    COALESCE(item_refund_in_usd, 0)                 AS item_refund_usd,
    COALESCE(item_revenue_in_usd, 0)
        - COALESCE(item_refund_in_usd, 0)           AS net_revenue_usd,

    -- Promotions
    coupon,
    promotion_id,
    promotion_name,

    -- Merchandising context
    item_list_id,
    item_list_name,
    item_list_index,
    creative_name,
    creative_slot,
    affiliation

FROM {{ source('raw', 'raw_purchase_items') }}

{% if is_incremental() %}
WHERE event_date_dt > (SELECT MAX(purchase_date) FROM {{ this }})
{% endif %}
