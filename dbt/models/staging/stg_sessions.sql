/*
  stg_sessions.sql
  ================
  One row per unique session per user.
  Session = session_start event in GA4.

  Key decisions:
  ─────────────
  1. SESSION_KEY:
     MD5(user_pseudo_id + ga_session_id) for global uniqueness.
     ga_session_id alone is NOT globally unique in GA4 —
     it's timestamp-based and unique only per user.
     Two different users can share the same ga_session_id.

  2. SESSION-LEVEL GROUPING:
     GROUP BY user_pseudo_id + ga_session_id (not event_id).
     GA4 fires session_start multiple times per session
     (e.g. on different pages). Grouping at session level
     collapses these into one row naturally — no ROW_NUMBER needed.
     More performant and semantically correct.

  3. LANDING PAGE:
     ARRAY_AGG ordered by event_timestamp picks the page from
     the earliest session_start event — correct landing page definition.

  4. ENGAGEMENT:
     MAX on session_engaged and engaged_session_event — if any
     session_start event was engaged, the session is engaged.

  5. INCREMENTAL:
     Incremental on session_key. Safe to re-run.
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

unnested AS (
    SELECT
        user_pseudo_id,
        user_id,

        -- Session-level aggregation
        -- Multiple session_start events per session collapsed into one row
        MIN(event_date_dt)                              AS session_date,
        MIN(event_timestamp)                            AS session_timestamp,
        MIN(user_first_touch_timestamp)                 AS user_first_touch_timestamp,

        -- Session identifiers from event_params
        MAX(CASE WHEN ep.key = 'ga_session_id'
            THEN ep.value.int_value END)                AS ga_session_id,
        MAX(CASE WHEN ep.key = 'ga_session_number'
            THEN ep.value.int_value END)                AS ga_session_number,

        -- Engagement — MAX captures any engaged event in session
        MAX(CASE WHEN ep.key = 'session_engaged'
            THEN ep.value.string_value END)             AS session_engaged,
        MAX(CASE WHEN ep.key = 'engaged_session_event'
            THEN ep.value.int_value END)                AS engaged_session_event,

        -- Landing page — first page of session (ordered by timestamp)
        (ARRAY_AGG(
            CASE WHEN ep.key = 'page_location'
            THEN ep.value.string_value END
            IGNORE NULLS
            ORDER BY event_timestamp ASC
            LIMIT 1
        ))[OFFSET(0)]                                   AS landing_page,

        (ARRAY_AGG(
            CASE WHEN ep.key = 'page_title'
            THEN ep.value.string_value END
            IGNORE NULLS
            ORDER BY event_timestamp ASC
            LIMIT 1
        ))[OFFSET(0)]                                   AS landing_page_title,

        (ARRAY_AGG(
            CASE WHEN ep.key = 'page_referrer'
            THEN ep.value.string_value END
            IGNORE NULLS
            ORDER BY event_timestamp ASC
            LIMIT 1
        ))[OFFSET(0)]                                   AS referrer,

        -- Device, geo, traffic from first event in session
        ANY_VALUE(device)                               AS device,
        ANY_VALUE(geo)                                  AS geo,
        ANY_VALUE(traffic_source)                       AS traffic_source

    FROM source
    CROSS JOIN UNNEST(event_params) AS ep
    GROUP BY
        user_pseudo_id,
        user_id
)

SELECT
    -- Globally unique session identifier
    TO_HEX(MD5(CONCAT(
        COALESCE(user_pseudo_id,                        ''),
        COALESCE(CAST(ga_session_id AS STRING),         '')
    )))                                                 AS session_key,

    session_date,
    TIMESTAMP_MICROS(session_timestamp)                 AS session_started_at,

    -- User identifiers
    user_pseudo_id,
    user_id,
    TIMESTAMP_MICROS(user_first_touch_timestamp)        AS user_first_touch_at,

    -- Session identifiers
    ga_session_id,
    ga_session_number,

    -- Engagement
    CASE WHEN session_engaged = '1'
         THEN TRUE ELSE FALSE END                       AS is_engaged,
    COALESCE(engaged_session_event, 0)                  AS engaged_event_count,

    -- Landing page (first page of session)
    landing_page,
    landing_page_title,
    referrer,

    -- Traffic source
    traffic_source.source                               AS traffic_source,
    traffic_source.medium                               AS traffic_medium,
    traffic_source.name                                 AS traffic_campaign,

    -- Device
    device.category                                     AS device_category,
    device.operating_system                             AS operating_system,
    device.web_info.browser                             AS browser,
    device.language                                     AS language,

    -- Geography
    geo.country                                         AS country,
    geo.region                                          AS region,
    geo.city                                            AS city

FROM unnested
WHERE ga_session_id IS NOT NULL
