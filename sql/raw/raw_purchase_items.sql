/*
  raw_purchase_items.sql
  ======================
  Merges one day of GA4 purchase items from the public dataset
  into the raw_purchase_items table.

  Called by: extract.py
  Parameters:
    {project}  — GCP project ID
    {dataset}  — BigQuery dataset name
    {date}     — format YYYYMMDD (e.g. 20201101)

  Design decisions:
  ─────────────────
  1. GRAIN:
     One row per item per transaction per day.
     Items array is UNNESTED here (not in staging) because
     items represent a fundamentally different grain than events.
     This is a structural split, not a transformation decision.

  2. SURROGATE KEY (item_event_id):
     MD5 hash of date + transaction_id + user_pseudo_id +
     event_timestamp + UNNEST offset.
     - OFFSET used instead of item_list_index: offset is a
       guaranteed unique sequential integer from BigQuery UNNEST,
       item_list_index is a merchandising field that can be NULL
       or non-unique in edge cases.
     - user_pseudo_id + event_timestamp included to guard against
       NULL transaction_id which GA4 can fire in some implementations.

  3. EVENT_ID INCLUDED:
     Same hash logic as raw_events.event_id included here to enable
     clean joins between raw_purchase_items and raw_events in
     downstream models without relying on nullable transaction_id.

  4. CTE BEFORE UNNEST:
     Purchases filtered and columns projected in CTE before CROSS JOIN.
     BigQuery reads only 5 columns from purchase events (5,692 rows)
     instead of all 80+ columns from all events (3.7M rows).
     Significant cost optimization.

  5. PARTITION GUARD:
     Static literal PARSE_DATE('%Y%m%d', '{date}') forces BigQuery
     to prune target to exactly one partition before evaluating
     item_event_id — same pattern as raw_events.sql.

  6. IMMUTABLE RAW LAYER:
     MERGE only inserts, never updates. Raw data is append-only.
*/

MERGE INTO `{project}.{dataset}.raw_purchase_items` AS target

USING (
    -- CTE filters to purchase events and projects only needed columns
    -- BEFORE UNNEST to minimize data scanned
    WITH purchases AS (
        SELECT
            event_date, -- partition key
            event_name, -- To include it in event_id hash
            event_timestamp, -- surrogate key   
            event_bundle_sequence_id, -- surrogate key
            user_pseudo_id, -- user identifier
            ecommerce, -- transaction reference
            items -- item details
        FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_{date}`
        WHERE event_name = 'purchase'
    )

    SELECT
        -- Partition key
        PARSE_DATE('%Y%m%d', p.event_date)              AS event_date_dt,

        -- Surrogate key for deduplication (item_event_id)
        -- OFFSET guarantees uniqueness per item within transaction
        -- user_pseudo_id + event_timestamp guard against NULL transaction_id
        TO_HEX(MD5(CONCAT(
            COALESCE(p.event_date,                                      ''),
            COALESCE(p.ecommerce.transaction_id,                       ''),
            COALESCE(p.user_pseudo_id,                                 ''),
            COALESCE(CAST(p.event_timestamp AS STRING),                ''),
            COALESCE(CAST(item_offset AS STRING),                      '0') -- '0' not '' to distinguish NULL offset from empty string in hash
        )))                                             AS item_event_id,

        -- Event reference key (same hash as raw_events.event_id)
        -- Enables clean joins to raw_events without nullable transaction_id
        TO_HEX(MD5(CONCAT(
            COALESCE(p.event_date,                                      ''),
            COALESCE(p.user_pseudo_id,                                 ''),
            COALESCE(CAST(p.event_timestamp AS STRING),                ''),
            COALESCE(p.event_name,                                     ''),
            COALESCE(CAST(p.event_bundle_sequence_id AS STRING),       '0')
        )))                                             AS event_id,

        -- Transaction reference
        p.ecommerce.transaction_id                      AS transaction_id,

        -- User identifier
        p.user_pseudo_id                                AS user_pseudo_id,

        item_offset                                     AS item_position, -- item position within transaction array like 1, 2, 3, etc.

        -- Item details
        item.item_id                                    AS item_id, -- item identifier like "1234567890"
        item.item_name                                  AS item_name, -- item name like "T-Shirt"
        item.item_brand                                 AS item_brand, -- item brand like "Nike"
        item.item_variant                               AS item_variant, -- item variant like "Small"
        item.item_category                              AS item_category, -- item category like "Clothing"
        item.item_category2                             AS item_category2, -- item category2 like "Men's Clothing"
        item.item_category3                             AS item_category3, -- item category3 like "T-Shirts"

        -- Pricing and quantity
        item.price_in_usd                               AS price_in_usd, -- price in USD
        item.price                                      AS price_local, -- price in local currency
        item.quantity                                   AS quantity, -- quantity of items purchased

        -- Revenue and refunds
        item.item_revenue_in_usd                        AS item_revenue_in_usd,
        item.item_revenue                               AS item_revenue_local,
        item.item_refund_in_usd                         AS item_refund_in_usd,
        item.item_refund                                AS item_refund_local,

        -- Promotion and list context
        -- Captured for completeness — useful for merchandising analysis
        item.coupon                                     AS coupon, -- coupon code like "SUMMER10"
        item.affiliation                                AS affiliation, -- affiliation like "Store 123"
        item.item_list_id                               AS item_list_id, -- item list identifier like "1234567890"
        item.item_list_name                             AS item_list_name, -- item list name like "Shopping Cart"   
        item.item_list_index                            AS item_list_index, -- item list index like 1, 2, 3, etc.
        item.promotion_id                               AS promotion_id, -- promotion identifier like "1234567890"
        item.promotion_name                             AS promotion_name, -- promotion name like "Summer Sale"
        item.creative_name                              AS creative_name, -- creative name like "Summer Sale Banner"
        item.creative_slot                              AS creative_slot, -- creative slot like "Banner"

        -- Ingestion metadata
        CURRENT_TIMESTAMP()                             AS ingested_at, -- timestamp of ingestion
        '{date}'                                        AS source_table_suffix

    FROM purchases p
    CROSS JOIN UNNEST(p.items) AS item WITH OFFSET AS item_offset

) AS source

-- Partition guard: static literal forces BigQuery to prune
-- target to exactly one partition before evaluating item_event_id
ON  target.event_date_dt  = PARSE_DATE('%Y%m%d', '{date}')
AND target.item_event_id  = source.item_event_id

-- Raw layer is immutable — insert only, never update
WHEN NOT MATCHED THEN INSERT (
    event_date_dt,
    item_event_id,
    event_id,
    transaction_id,
    user_pseudo_id,
    item_position,
    item_id,
    item_name,
    item_brand,
    item_variant,
    item_category,
    item_category2,
    item_category3,
    price_in_usd,
    price_local,
    quantity,
    item_revenue_in_usd,
    item_revenue_local,
    item_refund_in_usd,
    item_refund_local,
    coupon,
    affiliation,
    item_list_id,
    item_list_name,
    item_list_index,
    promotion_id,
    promotion_name,
    creative_name,
    creative_slot,
    ingested_at,
    source_table_suffix
)
VALUES (
    source.event_date_dt,
    source.item_event_id,
    source.event_id,
    source.transaction_id,
    source.user_pseudo_id,
    source.item_position,
    source.item_id,
    source.item_name,
    source.item_brand,
    source.item_variant,
    source.item_category,
    source.item_category2,
    source.item_category3,
    source.price_in_usd,
    source.price_local,
    source.quantity,
    source.item_revenue_in_usd,
    source.item_revenue_local,
    source.item_refund_in_usd,
    source.item_refund_local,
    source.coupon,
    source.affiliation,
    source.item_list_id,
    source.item_list_name,
    source.item_list_index,
    source.promotion_id,
    source.promotion_name,
    source.creative_name,
    source.creative_slot,
    source.ingested_at,
    source.source_table_suffix
);