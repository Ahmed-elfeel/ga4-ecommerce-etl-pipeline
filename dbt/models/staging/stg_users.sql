/*
  stg_users.sql
  =============
  One row per unique user (user_pseudo_id).
  Full table rebuild — dimension table, not incremental.

  Key decisions:
  - Materialized as table (not incremental) — user attributes
    like first_seen_date require scanning all history to be accurate.
    Incremental would miss users whose first event was in an
    already-processed partition.
  - Built from session_start events only — most reliable signal
    for first user appearance
  - user_pseudo_id as primary key
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
        ep.key                                      AS param_key,
        ep.value.int_value                          AS param_int_value,
        ep.value.string_value                       AS param_string_value

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
            THEN param_int_value END)               AS ga_session_id,
        MAX(CASE WHEN param_key = 'ga_session_number'
            THEN param_int_value END)               AS ga_session_number

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
    -- Get attributes from the user's very first session
    SELECT
        user_pseudo_id,
        MIN(event_date_dt)                          AS first_seen_date,
        MIN(event_timestamp)                        AS first_seen_timestamp,
        MAX(event_date_dt)                          AS last_seen_date,
        COUNT(DISTINCT TO_HEX(MD5(CONCAT(
            COALESCE(user_pseudo_id, ''),
            COALESCE(CAST(ga_session_id AS STRING), '')
        ))))                                        AS total_sessions,
        MAX(user_first_touch_timestamp)             AS user_first_touch_timestamp

    FROM pivoted
    GROUP BY user_pseudo_id
),

first_touch_attributes AS (
    -- Traffic source, device, geo from user's first ever session
    SELECT DISTINCT
        user_pseudo_id,
        FIRST_VALUE(traffic_source.source)
            OVER (PARTITION BY user_pseudo_id
                  ORDER BY event_timestamp ASC)     AS acquisition_source,
        FIRST_VALUE(traffic_source.medium)
            OVER (PARTITION BY user_pseudo_id
                  ORDER BY event_timestamp ASC)     AS acquisition_medium,
        FIRST_VALUE(traffic_source.name)
            OVER (PARTITION BY user_pseudo_id
                  ORDER BY event_timestamp ASC)     AS acquisition_campaign,
        FIRST_VALUE(device.category)
            OVER (PARTITION BY user_pseudo_id
                  ORDER BY event_timestamp ASC)     AS first_device_category,
        FIRST_VALUE(geo.country)
            OVER (PARTITION BY user_pseudo_id
                  ORDER BY event_timestamp ASC)     AS country,
        FIRST_VALUE(geo.city)
            OVER (PARTITION BY user_pseudo_id
                  ORDER BY event_timestamp ASC)     AS city

    FROM pivoted
)

SELECT
    fs.user_pseudo_id,

    -- First and last activity
    fs.first_seen_date,
    TIMESTAMP_MICROS(fs.first_seen_timestamp)       AS first_seen_at,
    fs.last_seen_date,
    TIMESTAMP_MICROS(fs.user_first_touch_timestamp) AS user_first_touch_at,

    -- Engagement summary
    fs.total_sessions,

    -- Acquisition attributes (from first session)
    fta.acquisition_source,
    fta.acquisition_medium,
    fta.acquisition_campaign,
    fta.first_device_category,
    fta.country,
    fta.city

FROM first_session fs
LEFT JOIN first_touch_attributes fta
    ON fs.user_pseudo_id = fta.user_pseudo_id
