"""
extract.py
==========
Extracts GA4 ecommerce data from the public BigQuery dataset
and loads it into the raw layer of our data warehouse.

Usage:
    python python/extract.py                    # auto-discover missing/incomplete dates
    python python/extract.py --date 20201101    # force specific date (bypasses auto-discovery)

Design decisions:
─────────────────
1. BOOTSTRAP TABLE CREATION:
   Tables created using CREATE TABLE IF NOT EXISTS ... AS SELECT ... LIMIT 0
   against the actual source. Schema always matches source exactly.
   No manual SchemaField definitions — eliminates all type mismatch errors.
   Partitioning and clustering added explicitly after schema inference.

2. DATE DISCOVERY:
   Uses pipeline_runs as source of truth — not raw row presence.
   Detects incomplete dates by comparing source vs raw row counts.
   Full range scan for historical datasets (lookback_days=None).
   In production, set lookback_days=3 after initial backfill.

3. COMPLETENESS CHECK:
   Compares source vs raw row counts per date.
   Only re-processes dates where counts differ.
   More efficient than fixed rolling window.

4. MANUAL OVERRIDE:
   --date flag bypasses auto-discovery entirely.
   Forces MERGE for that specific date.
   Useful for backfill and debugging.

5. ERROR ISOLATION:
   One date failing does not stop other dates from processing.
   Each date logged independently in pipeline_runs.

6. TIMEZONE AWARE:
   All timestamps use datetime.now(timezone.utc) instead of
   deprecated datetime.utcnow() for Python 3.12+ compatibility.

7. PARAMETERIZED LOGGING:
   pipeline_runs INSERT uses BigQuery query parameters.
   Handles all special characters in error messages safely.
   No string escaping needed — BigQuery handles it internally.
"""

import argparse
import logging
import sys
import uuid
from datetime import datetime, date, timezone

import yaml
from google.cloud import bigquery
from google.oauth2 import service_account


# ─────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
def load_config(config_path: str = 'config/config.yaml') -> dict:
    """Load project configuration from YAML file."""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# ─────────────────────────────────────────────
# BIGQUERY CLIENT
# ─────────────────────────────────────────────
def get_bq_client(config: dict) -> bigquery.Client:
    """Initialize BigQuery client using service account credentials."""
    credentials = service_account.Credentials.from_service_account_file(
        config['credentials']['service_account_path'],
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(
        credentials=credentials,
        project=config['project']['gcp_project_id']
    )


# ─────────────────────────────────────────────
# TABLE CREATION — BOOTSTRAP PATTERN
# ─────────────────────────────────────────────
def create_raw_events_if_not_exists(
    client: bigquery.Client,
    config: dict
) -> None:
    """
    Creates raw_events table using source schema as template.

    Bootstrap pattern: CREATE TABLE IF NOT EXISTS ... AS SELECT ... LIMIT 0
    Schema is inferred directly from the source — no manual definition needed.
    This eliminates all type mismatch errors permanently.

    Additions over source schema:
    - event_date_dt (DATE): replaces event_date (STRING) as partition key
    - event_id (STRING): MD5 surrogate key for MERGE deduplication

    Partitioned by event_date_dt.
    Clustered by event_name + user_pseudo_id.
    """
    project = config['project']['gcp_project_id']
    dataset = config['project']['dataset']

    sql = """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.raw_events`
        PARTITION BY event_date_dt
        CLUSTER BY event_name, user_pseudo_id
        AS SELECT
            -- Partition key: DATE type replaces STRING event_date
            PARSE_DATE('%Y%m%d', event_date)        AS event_date_dt,

            -- Surrogate key for MERGE deduplication
            TO_HEX(MD5(CONCAT(
                COALESCE(event_date,                                ''),
                COALESCE(user_pseudo_id,                           ''),
                COALESCE(CAST(event_timestamp AS STRING),          ''),
                COALESCE(event_name,                               ''),
                COALESCE(CAST(event_bundle_sequence_id AS STRING), '0')
            )))                                     AS event_id,

            -- All source fields except event_date (replaced by event_date_dt)
            -- Schema inherited exactly from source — no type mismatches possible
            * EXCEPT(event_date),

            -- Ingestion metadata
            CURRENT_TIMESTAMP()                     AS ingested_at,
            event_date                              AS source_table_suffix

        FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20201101`
        LIMIT 0
    """.format(project=project, dataset=dataset)

    job = client.query(sql)
    job.result()
    log.info(f"raw_events table ready")


def create_raw_purchase_items_if_not_exists(
    client: bigquery.Client,
    config: dict
) -> None:
    """
    Creates raw_purchase_items table using source schema as template.

    Bootstrap pattern: CREATE TABLE IF NOT EXISTS ... AS SELECT ... LIMIT 0
    Item fields projected explicitly — not item.* — to prevent silent schema
    divergence if GA4 adds new item fields in the future.

    Additions over source schema:
    - event_date_dt (DATE): partition key
    - item_event_id (STRING): MD5 surrogate key for MERGE deduplication
    - event_id (STRING): same hash as raw_events.event_id for cross-table joins
    - transaction_id (STRING): promoted from ecommerce struct for easy access
    - user_pseudo_id (STRING): copied from event level
    - item_position (INT64): UNNEST offset for row uniqueness within transaction

    Partitioned by event_date_dt.
    Clustered by item_name + transaction_id.
    """
    project = config['project']['gcp_project_id']
    dataset = config['project']['dataset']

    sql = """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.raw_purchase_items`
        PARTITION BY event_date_dt
        CLUSTER BY item_name, transaction_id
        AS SELECT
            -- Partition key
            PARSE_DATE('%Y%m%d', event_date)            AS event_date_dt,

            -- Surrogate key for item-level deduplication
            -- CAST(0 AS INT64) ensures INT64 type matches production MERGE
            -- which uses item_offset (INT64) from UNNEST WITH OFFSET
            TO_HEX(MD5(CONCAT(
                COALESCE(event_date,                                    ''),
                COALESCE(ecommerce.transaction_id,                     ''),
                COALESCE(user_pseudo_id,                               ''),
                COALESCE(CAST(event_timestamp AS STRING),              ''),
                COALESCE(CAST(CAST(0 AS INT64) AS STRING),             '0')
            )))                                         AS item_event_id,

            -- Event reference key — same hash as raw_events.event_id
            -- Enables clean joins without relying on nullable transaction_id
            TO_HEX(MD5(CONCAT(
                COALESCE(event_date,                                    ''),
                COALESCE(user_pseudo_id,                               ''),
                COALESCE(CAST(event_timestamp AS STRING),              ''),
                COALESCE(event_name,                                   ''),
                COALESCE(CAST(event_bundle_sequence_id AS STRING),     '0')
            )))                                         AS event_id,

            -- Transaction reference
            ecommerce.transaction_id                    AS transaction_id,

            -- User identifier
            user_pseudo_id,

            -- Item position within transaction array
            -- CAST(0 AS INT64) explicitly typed to match production MERGE
            -- which inserts item_offset (INT64) from UNNEST WITH OFFSET
            CAST(0 AS INT64)                            AS item_position,

            -- Item fields projected explicitly (not item.*)
            -- Explicit projection prevents silent schema divergence
            -- if GA4 adds new item fields in the future
            item.item_id,
            item.item_name,
            item.item_brand,
            item.item_variant,
            item.item_category,
            item.item_category2,
            item.item_category3,
            item.price_in_usd,
            item.price,
            item.quantity,
            item.item_revenue_in_usd,
            item.item_revenue,
            item.item_refund_in_usd,
            item.item_refund,
            item.coupon,
            item.affiliation,
            item.item_list_id,
            item.item_list_name,
            item.item_list_index,
            item.promotion_id,
            item.promotion_name,
            item.creative_name,
            item.creative_slot,

            -- Ingestion metadata
            CURRENT_TIMESTAMP()                         AS ingested_at,
            event_date                                  AS source_table_suffix

        FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20201101`
        CROSS JOIN UNNEST(items) AS item
        WHERE event_name = 'purchase'
        LIMIT 0
    """.format(project=project, dataset=dataset)

    job = client.query(sql)
    job.result()
    log.info(f"raw_purchase_items table ready")


def create_pipeline_runs_if_not_exists(
    client: bigquery.Client,
    config: dict
) -> None:
    """
    Creates pipeline_runs monitoring table.
    Schema defined explicitly — this table has no source to inherit from.
    """
    project   = config['project']['gcp_project_id']
    dataset   = config['project']['dataset']
    log_table = config['pipeline']['log_table']

    sql = """
        CREATE TABLE IF NOT EXISTS `{project}.{dataset}.{log_table}` (
            run_id           STRING    NOT NULL,
            run_date         DATE,
            layer            STRING,
            status           STRING,
            rows_processed   INT64,
            error_message    STRING,
            started_at       TIMESTAMP,
            completed_at     TIMESTAMP,
            duration_seconds FLOAT64
        )
    """.format(
        project=project,
        dataset=dataset,
        log_table=log_table
    )

    job = client.query(sql)
    job.result()
    log.info(f"pipeline_runs table ready")


def ensure_raw_tables_exist(
    client: bigquery.Client,
    config: dict
) -> None:
    """
    Ensures all raw tables exist before running MERGE.
    Uses bootstrap pattern — schema inherited from source.
    Safe to call on every run (IF NOT EXISTS is idempotent).
    """
    create_raw_events_if_not_exists(client, config)
    create_raw_purchase_items_if_not_exists(client, config)
    create_pipeline_runs_if_not_exists(client, config)


# ─────────────────────────────────────────────
# DATE DISCOVERY
# ─────────────────────────────────────────────
def get_source_dates(
    client: bigquery.Client,
    config: dict
) -> set:
    """
    Returns all available dates in the source GA4 dataset.
    Uses _TABLE_SUFFIX to list available daily shards.
    """
    query = """
        SELECT DISTINCT _TABLE_SUFFIX AS date_suffix
        FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY date_suffix
    """.format(
        start_date=config['source']['start_date'],
        end_date=config['source']['end_date']
    )

    results = client.query(query).result()
    dates = {row.date_suffix for row in results}
    log.info(f"Found {len(dates)} dates in source dataset")
    return dates


def get_completed_dates(
    client: bigquery.Client,
    config: dict
) -> set:
    """
    Returns dates successfully processed.
    Uses pipeline_runs as source of truth — not raw row presence.
    A date is complete only if it has a successful pipeline run.
    """
    project   = config['project']['gcp_project_id']
    dataset   = config['project']['dataset']
    log_table = config['pipeline']['log_table']

    query = """
        SELECT DISTINCT FORMAT_DATE('%Y%m%d', run_date) AS date_suffix
        FROM `{project}.{dataset}.{log_table}`
        WHERE layer  = 'raw'
        AND   status = 'success'
    """.format(
        project=project,
        dataset=dataset,
        log_table=log_table
    )

    try:
        results = client.query(query).result()
        dates = {row.date_suffix for row in results}
        log.info(f"Found {len(dates)} completed dates in pipeline_runs")
        return dates
    except Exception:
        log.info("pipeline_runs empty — first run detected")
        return set()


def get_incomplete_dates(
    client: bigquery.Client,
    config: dict,
    lookback_days: int = None
) -> set:
    """
    Detects dates with incomplete data by comparing source
    vs raw row counts.

    lookback_days=None: scans full date range from config.
                        Correct for historical datasets.
    lookback_days=N:    scans only last N days.
                        Use in production for cost efficiency
                        once historical backfill is complete.

    Note: For this assignment, full range scan is correct.
    In production, set lookback_days=3 after initial backfill.
    """
    project = config['project']['gcp_project_id']
    dataset = config['project']['dataset']

    if lookback_days is not None:
        from datetime import timedelta
        end_date   = date.today().strftime('%Y%m%d')
        start_date = (
            date.today() - timedelta(days=lookback_days)
        ).strftime('%Y%m%d')
        log.info(
            f"Checking incomplete dates for last "
            f"{lookback_days} days: {start_date} to {end_date}"
        )
    else:
        start_date = config['source']['start_date']
        end_date   = config['source']['end_date']
        log.info(
            f"Checking incomplete dates for full range: "
            f"{start_date} to {end_date}"
        )

    query = """
        WITH source_counts AS (
            SELECT
                _TABLE_SUFFIX   AS date_suffix,
                COUNT(*)        AS source_rows
            FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
            WHERE _TABLE_SUFFIX BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY _TABLE_SUFFIX
        ),
        raw_counts AS (
            SELECT
                FORMAT_DATE('%Y%m%d', event_date_dt)    AS date_suffix,
                COUNT(*)                                AS raw_rows
            FROM `{project}.{dataset}.raw_events`
            WHERE event_date_dt BETWEEN
                PARSE_DATE('%Y%m%d', '{start_date}') AND
                PARSE_DATE('%Y%m%d', '{end_date}')
            GROUP BY date_suffix
        )
        SELECT s.date_suffix
        FROM source_counts s
        LEFT JOIN raw_counts r ON s.date_suffix = r.date_suffix
        WHERE r.raw_rows IS NULL
           OR s.source_rows > r.raw_rows
    """.format(
        project=project,
        dataset=dataset,
        start_date=start_date,
        end_date=end_date
    )

    try:
        results = client.query(query).result()
        dates = {row.date_suffix for row in results}
        if dates:
            log.info(
                f"Found {len(dates)} incomplete dates: "
                f"{sorted(dates)}"
            )
        return dates
    except Exception:
        return set()


def get_dates_to_process(
    client: bigquery.Client,
    config: dict,
    override_date: str = None
) -> list:
    """
    Determines which dates to process.

    If override_date provided:
        Returns [override_date] — bypasses all discovery logic.
        Forces MERGE for that specific date regardless of status.

    If no override:
        Returns union of:
        - Missing dates: never been processed
        - Incomplete dates: processed but source has more rows than raw
        Sorted oldest first for chronological processing.
    """
    if override_date:
        log.info(f"Manual override: processing {override_date} only")
        return [override_date]

    source_dates     = get_source_dates(client, config)
    completed_dates  = get_completed_dates(client, config)
    missing_dates    = source_dates - completed_dates
    incomplete_dates = get_incomplete_dates(client, config)

    dates_to_process = missing_dates | incomplete_dates
    sorted_dates     = sorted(dates_to_process)

    log.info(
        f"Dates to process: {len(sorted_dates)} "
        f"({len(missing_dates)} missing, "
        f"{len(incomplete_dates)} incomplete)"
    )
    return sorted_dates


# ─────────────────────────────────────────────
# SQL EXECUTION
# ─────────────────────────────────────────────
def load_sql(sql_path: str) -> str:
    """Load SQL template from file."""
    with open(sql_path, 'r') as f:
        return f.read()


def run_merge_for_date(
    client: bigquery.Client,
    config: dict,
    target_date: str,
    sql_template: str
) -> int:
    """
    Runs MERGE SQL for a specific date.
    Returns number of rows affected.
    """
    project = config['project']['gcp_project_id']
    dataset = config['project']['dataset']

    sql = sql_template.format(
        project=project,
        dataset=dataset,
        date=target_date
    )

    job = client.query(sql)
    job.result()

    rows = job.num_dml_affected_rows or 0
    return rows


# ─────────────────────────────────────────────
# PIPELINE RUN LOGGING
# ─────────────────────────────────────────────
def log_pipeline_run(
    client: bigquery.Client,
    config: dict,
    run_date: str,
    status: str,
    rows_processed: int = 0,
    error_message: str = None,
    started_at: datetime = None,
    completed_at: datetime = None
) -> None:
    """
    Logs pipeline run to pipeline_runs table.
    Source of truth for date completion status.

    Uses BigQuery query parameters for the INSERT — handles all
    special characters in error messages safely without manual escaping.
    """
    project   = config['project']['gcp_project_id']
    dataset   = config['project']['dataset']
    log_table = config['pipeline']['log_table']

    duration = None
    if started_at and completed_at:
        duration = (completed_at - started_at).total_seconds()

    insert_query = """
        INSERT INTO `{project}.{dataset}.{log_table}`
        (run_id, run_date, layer, status, rows_processed,
         error_message, started_at, completed_at, duration_seconds)
        VALUES
        (@run_id, @run_date, @layer, @status, @rows_processed,
         @error_message, @started_at, @completed_at, @duration_seconds)
    """.format(
        project=project,
        dataset=dataset,
        log_table=log_table
    )

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "run_id", "STRING", str(uuid.uuid4())),
            bigquery.ScalarQueryParameter(
                "run_date", "DATE",
                datetime.strptime(run_date, '%Y%m%d').date()),
            bigquery.ScalarQueryParameter(
                "layer", "STRING", "raw"),
            bigquery.ScalarQueryParameter(
                "status", "STRING", status),
            bigquery.ScalarQueryParameter(
                "rows_processed", "INT64", rows_processed or 0),
            bigquery.ScalarQueryParameter(
                "error_message", "STRING", error_message),
            bigquery.ScalarQueryParameter(
                "started_at", "TIMESTAMP",
                started_at.isoformat() if started_at else None),
            bigquery.ScalarQueryParameter(
                "completed_at", "TIMESTAMP",
                completed_at.isoformat() if completed_at else None),
            bigquery.ScalarQueryParameter(
                "duration_seconds", "FLOAT64", duration),
        ]
    )

    try:
        job = client.query(insert_query, job_config=job_config)
        job.result()
        log.info(f"Logged pipeline run: {status} for {run_date}")
    except Exception as e:
        log.warning(f"Failed to log pipeline run: {e}")


# ─────────────────────────────────────────────
# MAIN EXTRACTION FUNCTION
# ─────────────────────────────────────────────
def extract(
    config: dict,
    override_date: str = None
) -> dict:
    """
    Main extraction function.
    Orchestrates table creation, date discovery, and MERGE execution.
    Returns summary of results.
    """
    client = get_bq_client(config)

    # Step 1: Ensure all raw tables exist
    log.info("Ensuring raw tables exist...")
    ensure_raw_tables_exist(client, config)

    # Step 2: Determine dates to process
    dates = get_dates_to_process(client, config, override_date)

    if not dates:
        log.info("No dates to process — raw layer is up to date")
        return {'dates_processed': 0, 'total_rows': 0, 'errors': []}

    # Step 3: Load SQL templates
    raw_events_sql = load_sql('sql/raw/raw_events.sql')
    raw_items_sql  = load_sql('sql/raw/raw_purchase_items.sql')

    # Step 4: Process each date independently
    results = {
        'dates_processed': 0,
        'total_rows':      0,
        'errors':          []
    }

    for target_date in dates:
        log.info(f"Processing date: {target_date}")
        started_at = datetime.now(timezone.utc)

        try:
            # Run raw_events MERGE
            events_rows = run_merge_for_date(
                client, config, target_date, raw_events_sql
            )
            log.info(f"  raw_events: {events_rows:,} rows inserted")

            # Run raw_purchase_items MERGE
            items_rows = run_merge_for_date(
                client, config, target_date, raw_items_sql
            )
            log.info(f"  raw_purchase_items: {items_rows:,} rows inserted")

            total_rows   = events_rows + items_rows
            completed_at = datetime.now(timezone.utc)

            # Log success to pipeline_runs
            log_pipeline_run(
                client, config,
                run_date=target_date,
                status='success',
                rows_processed=total_rows,
                started_at=started_at,
                completed_at=completed_at
            )

            results['dates_processed'] += 1
            results['total_rows']      += total_rows

        except Exception as e:
            completed_at = datetime.now(timezone.utc)
            error_msg    = str(e)
            log.error(f"  Failed to process {target_date}: {error_msg}")

            # Log failure — does not stop other dates
            log_pipeline_run(
                client, config,
                run_date=target_date,
                status='failed',
                rows_processed=0,
                error_message=error_msg,
                started_at=started_at,
                completed_at=completed_at
            )

            results['errors'].append({
                'date':  target_date,
                'error': error_msg
            })

    log.info(
        f"Extraction complete: "
        f"{results['dates_processed']} dates processed, "
        f"{results['total_rows']:,} total rows, "
        f"{len(results['errors'])} errors"
    )
    return results


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Extract GA4 data into raw BigQuery tables'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Force processing of specific date (YYYYMMDD). '
             'Bypasses auto-discovery. Useful for backfill.',
        required=False
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to config file (default: config/config.yaml)'
    )
    args = parser.parse_args()

    config = load_config(args.config)

    results = extract(
        config=config,
        override_date=args.date
    )

    # Exit with non-zero code if any dates failed
    if results['errors']:
        log.error(
            f"Extraction completed with "
            f"{len(results['errors'])} errors"
        )
        sys.exit(1)

    sys.exit(0)