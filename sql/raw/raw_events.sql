/*
  raw_events.sql
  ==============
  Merges one day of GA4 events from the public dataset
  into the raw_events table.

  Called by: extract.py
  Parameters:
    {project}  — GCP project ID
    {dataset}  — BigQuery dataset name
    {date}     — format YYYYMMDD (e.g. 20201101)

  Design decisions:
  ─────────────────
  1. SURROGATE KEY (event_id):
     MD5 hash of date + user_pseudo_id + timestamp +
     event_name + bundle_sequence_id.
     Provides a single reliable deduplication key for
     MERGE and all downstream incremental models.

  2. NESTED STRUCTURES KEPT AS-IS:
     event_params and user_properties kept as repeated
     records. UNNEST happens in dbt staging layer.
     Structs (ecommerce, device, geo, traffic_source)
     kept as-is — BigQuery queries struct fields natively.

  3. COMPLETE SOURCE CAPTURE:
     All source fields included (platform, stream_id,
     privacy_info, app_info) even if not used in current
     metrics. Raw layer is a faithful copy of source.
     Filtering happens in staging and beyond.

  4. PARTITIONING + CLUSTERING:
     Partitioned by event_date_dt — eliminates full table
     scans on date-range queries.
     Clustered by event_name, user_pseudo_id — staging
     models filter heavily on event_name (purchase,
     session_start) and window on user_pseudo_id.

  5. IMMUTABLE RAW LAYER:
     MERGE only inserts, never updates. Raw data is
     append-only. If source data changes, re-ingest
     that date partition.

*/

MERGE INTO `{project}.{dataset}.raw_events` AS target

USING (
    SELECT
        -- Primary identifiers
        PARSE_DATE('%Y%m%d', event_date) AS event_date_dt, -- date of the event

        -- Surrogate key for deduplication
        -- Used as MERGE key and downstream unique_key (event_id)
        TO_HEX(MD5(CONCAT(
            COALESCE(event_date,                                    ''),
            COALESCE(user_pseudo_id,                               ''),
            COALESCE(CAST(event_timestamp AS STRING),              ''),
            COALESCE(event_name,                                   ''),
            COALESCE(CAST(event_bundle_sequence_id AS STRING), '0') -- '0' not '' to distinguish NULL bundle from empty string in hash
        )))                                        AS event_id,

        event_name, -- event name (e.g. 'purchase', 'view_item', 'add_to_cart')
        event_timestamp, -- timestamp of the event
        event_bundle_sequence_id, -- sequence ID for the event bundle

        -- User identifiers
        user_pseudo_id, -- anonymous user ID
        user_id, -- Google Analytics user ID (GA360 ID)
        user_first_touch_timestamp, -- first interaction with the property (e.g. first page view)

        -- Repeated records kept nested
        -- UNNEST happens in dbt staging layer
        event_params, -- event-level parameters
        user_properties, -- user-level properties

        -- Ecommerce struct kept as-is
        -- Fields accessible as ecommerce.transaction_id etc.
        ecommerce,

        -- Traffic source struct
        traffic_source,

        -- Device struct
        device,

        -- Geography struct
        geo,

        -- User lifetime value
        user_ltv,

        -- Platform and stream metadata
        -- Not used in current metrics but captured for completeness
        platform,
        stream_id,

        -- Privacy and consent info
        -- Important for GDPR compliance reporting
        privacy_info,

        -- App info (relevant for mobile GA4 implementations)
        app_info,

        -- Event dimensions
        event_dimensions, -- event dimensions (e.g. 'page_location', 'page_title')

        -- Ingestion metadata
        CURRENT_TIMESTAMP()                     AS ingested_at, -- timestamp of the ingestion
        '{date}'                                AS source_table_suffix -- suffix of the source table (YYYYMMDD)

    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_{date}`

) AS source

-- Partition guard: static literal forces BigQuery to prune
-- target to exactly one partition before evaluating event_id
ON  target.event_date_dt = PARSE_DATE('%Y%m%d', '{date}')
AND target.event_id      = source.event_id

-- Raw layer is immutable — insert only, never update
WHEN NOT MATCHED THEN INSERT (
    event_date_dt,
    event_id,
    event_name,
    event_timestamp,
    event_bundle_sequence_id,
    user_pseudo_id,
    user_id,
    user_first_touch_timestamp,
    event_params,
    user_properties,
    ecommerce,
    traffic_source,
    device,
    geo,
    user_ltv,
    platform,
    stream_id,
    privacy_info,
    app_info,
    event_dimensions,
    ingested_at,
    source_table_suffix
)
VALUES (
    source.event_date_dt,
    source.event_id,
    source.event_name,
    source.event_timestamp,
    source.event_bundle_sequence_id,
    source.user_pseudo_id,
    source.user_id,
    source.user_first_touch_timestamp,
    source.event_params,
    source.user_properties,
    source.ecommerce,
    source.traffic_source,
    source.device,
    source.geo,
    source.user_ltv,
    source.platform,
    source.stream_id,
    source.privacy_info,
    source.app_info,
    source.event_dimensions,
    source.ingested_at,
    source.source_table_suffix
);