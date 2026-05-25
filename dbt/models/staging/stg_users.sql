/*
  stg_users.sql
  =============
  One row per unique user (user_pseudo_id).
  Full table rebuild — dimension table, not incremental.

  Key decisions:
  - Materialized as table — user attributes like first_seen_date
    and first_purchase_date require scanning all history accurately.
  - Built from session_start events for session attributes.
  - first_purchase_date joined from stg_purchases for clean
    new vs returning customer calculation in mart layer.
  - user_pseudo_id as primary key.
*/

{{
    config(
        materialized='table'
    )
}}

WITH session_events AS (
    SELECT
        user_pseudo_id,
        event_date_dt,
        event_timestamp,
        user_first_touch_timestamp,
        traffic_source,
        device,
        geo,
        ep.key                                          AS param_key,
        ep.value.int_value                              AS param_int_value

    FROM {{ source('raw', 'raw_events') }}
    CROSS JOIN UNNEST(event_params) AS ep
    WHERE event_name = 'session_start'
    AND   ep.key IN ('ga_session_id', 'ga_session_number')
),

pivoted AS (
    SELECT
        user_pseudo_id,
        event_date_dt,
        event_timestamp,
        user_first_touch_timestamp,
        traffic_source,
        device,
        geo,
        MAX(CASE WHEN param_key = 'ga_session_id'
            THEN param_int_value END)                   AS ga_session_id,
        MAX(CASE WHEN param_key = 'ga_session_number'
            THEN param_int_value END)                   AS ga_session_number
    FROM session_events
    GROUP BY
        user_pseudo_id,
        event_date_dt,
        event_timestamp,
        user_first_touch_timestamp,
        traffic_source,
        device,
        geo
),

first_session AS (
    SELECT
        user_pseudo_id,
        MIN(event_date_dt)                              AS first_seen_date,
        MIN(event_timestamp)                            AS first_seen_timestamp,
        MAX(event_date_dt)                              AS last_seen_date,
        COUNT(DISTINCT TO_HEX(MD5(CONCAT(
            COALESCE(user_pseudo_id, ''),
            COALESCE(CAST(ga_session_id AS STRING), '')
        ))))                                            AS total_sessions,
        MAX(user_first_touch_timestamp)                 AS user_first_touch_timestamp
    FROM pivoted
    GROUP BY user_pseudo_id
),

first_touch_attributes AS (
    SELECT DISTINCT
        user_pseudo_id,
        FIRST_VALUE(traffic_source.source)
            OVER (PARTITION BY user_pseudo_id
                  ORDER BY event_timestamp ASC)         AS acquisition_source,
        FIRST_VALUE(traffic_source.medium)
            OVER (PARTITION BY user_pseudo_id
                  ORDER BY event_timestamp ASC)         AS acquisition_medium,
        FIRST_VALUE(traffic_source.name)
            OVER (PARTITION BY user_pseudo_id
                  ORDER BY event_timestamp ASC)         AS acquisition_campaign,
        FIRST_VALUE(device.category)
            OVER (PARTITION BY user_pseudo_id
                  ORDER BY event_timestamp ASC)         AS first_device_category,
        FIRST_VALUE(geo.country)
            OVER (PARTITION BY user_pseudo_id
                  ORDER BY event_timestamp ASC)         AS country,
        FIRST_VALUE(geo.city)
            OVER (PARTITION BY user_pseudo_id
                  ORDER BY event_timestamp ASC)         AS city
    FROM pivoted
),

-- First purchase date per user — used for new vs returning in mart
first_purchase AS (
    SELECT
        user_pseudo_id,
        MIN(purchase_date)                              AS first_purchase_date
    FROM {{ ref('stg_purchases') }}
    GROUP BY user_pseudo_id
)

SELECT
    all_users.user_pseudo_id,
    fs.first_seen_date,
    TIMESTAMP_MICROS(fs.first_seen_timestamp)           AS first_seen_at,
    fs.last_seen_date,
    TIMESTAMP_MICROS(fs.user_first_touch_timestamp)     AS user_first_touch_at,
    fs.total_sessions,

    -- First purchase date — NULL if user never purchased
    fp.first_purchase_date,

    -- Acquisition attributes
    fta.acquisition_source,
    fta.acquisition_medium,
    fta.acquisition_campaign,
    fta.first_device_category,
    fta.country,
    fta.city

FROM (
    SELECT DISTINCT user_pseudo_id FROM first_session
    UNION DISTINCT
    SELECT DISTINCT user_pseudo_id FROM first_purchase
) all_users
LEFT JOIN first_session fs          ON all_users.user_pseudo_id = fs.user_pseudo_id
LEFT JOIN first_touch_attributes fta ON all_users.user_pseudo_id = fta.user_pseudo_id
LEFT JOIN first_purchase fp          ON all_users.user_pseudo_id = fp.user_pseudo_id
