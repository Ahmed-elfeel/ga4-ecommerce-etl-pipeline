/*
  stg_sessions.sql
  ================
  One row per unique session per user.
  Session = session_start event in GA4.

  Key decisions:
  ─────────────
  1. SESSION_KEY:
     MD5(user_pseudo_id + ga_session_id) for global uniqueness.
     ga_session_id alone is NOT globally unique in GA4.

  2. TWO-STEP AGGREGATION:
     Step 1: Extract ga_session_id per event (event-level unnest)
     Step 2: Group by user_pseudo_id + ga_session_id (session-level)
     This correctly produces one row per session, not one row per user.
     Previous bug: grouping by user_pseudo_id only collapsed all sessions
     for a user into one row, undercounting sessions by ~35%.

  3. LANDING PAGE:
     ARRAY_AGG ordered by timestamp picks page from earliest event.

  4. INCREMENTAL on session_key.
*/

{{
    config(
        materialized='incremental',
        unique_key='session_key',
        on_schema_change='sync_all_columns'
    )
}}

WITH source AS (
    SELECT *
    FROM {{ source('raw', 'raw_events') }}
    WHERE event_name = 'session_start'

    {% if is_incremental() %}
        AND event_date_dt > (SELECT MAX(session_date) FROM {{ this }})
    {% endif %}
),

-- Step 1: Extract ga_session_id at event level first
event_level AS (
    SELECT
        event_date_dt,
        event_id,
        event_timestamp,
        user_pseudo_id,
        user_id,
        user_first_touch_timestamp,
        device,
        geo,
        traffic_source,
        ep.key                                          AS param_key,
        ep.value.int_value                              AS param_int,
        ep.value.string_value                           AS param_str
    FROM source
    CROSS JOIN UNNEST(event_params) AS ep
    WHERE ep.key IN (
        'ga_session_id',
        'ga_session_number',
        'session_engaged',
        'engaged_session_event',
        'page_location',
        'page_title',
        'page_referrer'
    )
),

-- Step 2: Pivot params at event level
event_pivoted AS (
    SELECT
        event_date_dt,
        event_id,
        event_timestamp,
        user_pseudo_id,
        user_id,
        user_first_touch_timestamp,
        device,
        geo,
        traffic_source,
        MAX(CASE WHEN param_key = 'ga_session_id'
            THEN param_int END)                         AS ga_session_id,
        MAX(CASE WHEN param_key = 'ga_session_number'
            THEN param_int END)                         AS ga_session_number,
        MAX(CASE WHEN param_key = 'session_engaged'
            THEN param_str END)                         AS session_engaged,
        MAX(CASE WHEN param_key = 'engaged_session_event'
            THEN param_int END)                         AS engaged_session_event,
        MAX(CASE WHEN param_key = 'page_location'
            THEN param_str END)                         AS page_location,
        MAX(CASE WHEN param_key = 'page_title'
            THEN param_str END)                         AS page_title,
        MAX(CASE WHEN param_key = 'page_referrer'
            THEN param_str END)                         AS page_referrer
    FROM event_level
    GROUP BY
        event_date_dt,
        event_id,
        event_timestamp,
        user_pseudo_id,
        user_id,
        user_first_touch_timestamp,
        device,
        geo,
        traffic_source
),

-- Step 3: Group by session (user + ga_session_id) — one row per session
session_level AS (
    SELECT
        user_pseudo_id,
        user_id,
        ga_session_id,

        MIN(event_date_dt)                              AS session_date,
        MIN(event_timestamp)                            AS session_timestamp,
        MIN(user_first_touch_timestamp)                 AS user_first_touch_timestamp,

        MAX(ga_session_number)                          AS ga_session_number,
        MAX(session_engaged)                            AS session_engaged,
        MAX(engaged_session_event)                      AS engaged_session_event,

        -- Landing page = first page in session
        (ARRAY_AGG(page_location IGNORE NULLS
            ORDER BY event_timestamp ASC LIMIT 1))[OFFSET(0)] AS landing_page,
        (ARRAY_AGG(page_title IGNORE NULLS
            ORDER BY event_timestamp ASC LIMIT 1))[OFFSET(0)] AS landing_page_title,
        (ARRAY_AGG(page_referrer IGNORE NULLS
            ORDER BY event_timestamp ASC LIMIT 1))[OFFSET(0)] AS referrer,

        ANY_VALUE(device)                               AS device,
        ANY_VALUE(geo)                                  AS geo,
        ANY_VALUE(traffic_source)                       AS traffic_source

    FROM event_pivoted
    WHERE ga_session_id IS NOT NULL
    GROUP BY
        user_pseudo_id,
        user_id,
        ga_session_id
)

SELECT
    -- Globally unique session identifier
    TO_HEX(MD5(CONCAT(
        COALESCE(user_pseudo_id,                        ''),
        COALESCE(CAST(ga_session_id AS STRING),         '')
    )))                                                 AS session_key,

    session_date,
    TIMESTAMP_MICROS(session_timestamp)                 AS session_started_at,

    user_pseudo_id,
    user_id,
    TIMESTAMP_MICROS(user_first_touch_timestamp)        AS user_first_touch_at,

    ga_session_id,
    ga_session_number,

    CASE WHEN session_engaged = '1'
         THEN TRUE ELSE FALSE END                       AS is_engaged,
    COALESCE(engaged_session_event, 0)                  AS engaged_event_count,

    landing_page,
    landing_page_title,
    referrer,

    traffic_source.source                               AS traffic_source,
    traffic_source.medium                               AS traffic_medium,
    traffic_source.name                                 AS traffic_campaign,

    device.category                                     AS device_category,
    device.operating_system                             AS operating_system,
    device.web_info.browser                             AS browser,
    device.language                                     AS language,

    geo.country                                         AS country,
    geo.region                                          AS region,
    geo.city                                            AS city

FROM session_level
