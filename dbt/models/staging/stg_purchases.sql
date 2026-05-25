/*
  stg_purchases.sql
  =================
  One row per transaction (purchase event).

  Key decisions:
  - event_id as unique_key — one purchase event per transaction
  - Enriched with event_params (currency, value, tax, payment_type)
  - session_key calculated to enable joining to stg_sessions
  - ecommerce struct flattened for easier downstream use
*/

{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='sync_all_columns'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'raw_events') }}
    WHERE event_name = 'purchase'

    {% if is_incremental() %}
        AND event_date_dt > (SELECT MAX(event_date_dt) FROM {{ this }})
    {% endif %}
),

unnested AS (
    SELECT
        event_date_dt,
        event_id,
        event_timestamp,
        user_pseudo_id,
        user_id,
        ecommerce,
        device,
        geo,
        traffic_source,

        MAX(CASE WHEN ep.key = 'ga_session_id'
            THEN ep.value.int_value END)            AS ga_session_id,
        MAX(CASE WHEN ep.key = 'ga_session_number'
            THEN ep.value.int_value END)            AS ga_session_number,
        MAX(CASE WHEN ep.key = 'currency'
            THEN ep.value.string_value END)         AS currency,
        MAX(CASE WHEN ep.key = 'value'
            THEN ep.value.float_value END)          AS event_value,
        MAX(CASE WHEN ep.key = 'tax'
            THEN ep.value.float_value END)          AS tax,
        MAX(CASE WHEN ep.key = 'payment_type'
            THEN ep.value.string_value END)         AS payment_type,
        MAX(CASE WHEN ep.key = 'coupon'
            THEN ep.value.string_value END)         AS coupon,
        MAX(CASE WHEN ep.key = 'shipping_tier'
            THEN ep.value.string_value END)         AS shipping_tier

    FROM source
    CROSS JOIN UNNEST(event_params) AS ep
    GROUP BY
        event_date_dt,
        event_id,
        event_timestamp,
        user_pseudo_id,
        user_id,
        ecommerce,
        device,
        geo,
        traffic_source
)

SELECT
    -- Primary key
    event_id,

    event_date_dt                                   AS purchase_date,
    event_timestamp,
    TIMESTAMP_MICROS(event_timestamp)               AS purchased_at,

    -- Session reference key
    TO_HEX(MD5(CONCAT(
        COALESCE(user_pseudo_id,                    ''),
        COALESCE(CAST(ga_session_id AS STRING),     '')
    )))                                             AS session_key,

    -- User
    user_pseudo_id,
    user_id,

    -- Session context
    ga_session_id,
    ga_session_number,

    -- Transaction
    ecommerce.transaction_id                        AS transaction_id,
    COALESCE(currency, 'USD')                       AS currency,

    -- Revenue breakdown
    ecommerce.purchase_revenue_in_usd               AS revenue_usd,
    ecommerce.tax_value_in_usd                      AS tax_usd,
    ecommerce.shipping_value_in_usd                 AS shipping_usd,
    ecommerce.refund_value_in_usd                   AS refund_usd,
    COALESCE(tax, 0)                                AS tax_local,
    payment_type,
    coupon,
    shipping_tier,

    -- Items summary
    ecommerce.total_item_quantity                   AS total_items,
    ecommerce.unique_items                          AS unique_items,

    -- Traffic source
    traffic_source.source                           AS traffic_source,
    traffic_source.medium                           AS traffic_medium,

    -- Device
    device.category                                 AS device_category,

    -- Geography
    geo.country                                     AS country,
    geo.city                                        AS city

FROM unnested
WHERE ecommerce.transaction_id IS NOT NULL
