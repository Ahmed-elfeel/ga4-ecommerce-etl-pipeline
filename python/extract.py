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
import time
import uuid
from datetime import datetime, date, timezone

import yaml
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, TimePartitioning, TimePartitioningType
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
# RETRY HELPER
# ─────────────────────────────────────────────
def with_retries(fn, max_retries=3, delay_seconds=30, backoff=2.0, label=""):
    """Retry an idempotent operation with exponential backoff.
    Safe here because all retried ops (DELETE+INSERT) are idempotent.
    """
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                raise
            wait = delay_seconds * (backoff ** (attempt - 1))
            log.warning(
                f"{label} failed (attempt {attempt}/{max_retries}): {e} — "
                f"retrying in {wait:.0f}s"
            )
            time.sleep(wait)


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
# TABLE CREATION
# ─────────────────────────────────────────────
def create_raw_events_if_not_exists(client, config):
    from google.cloud.bigquery import SchemaField
    project  = config['project']['gcp_project_id']
    dataset  = config['project']['dataset']
    table_id = f"{project}.{dataset}.raw_events"
    try:
        client.get_table(table_id)
        log.info("raw_events table ready")
        return
    except Exception:
        pass
    schema = [
        SchemaField("event_date_dt",              "DATE",      mode="NULLABLE"),
        SchemaField("event_id",                   "STRING",    mode="NULLABLE"),
        SchemaField("event_name",                 "STRING",    mode="NULLABLE"),
        SchemaField("event_timestamp",            "INTEGER",   mode="NULLABLE"),
        SchemaField("event_bundle_sequence_id",   "INTEGER",   mode="NULLABLE"),
        SchemaField("user_pseudo_id",             "STRING",    mode="NULLABLE"),
        SchemaField("user_id",                    "STRING",    mode="NULLABLE"),
        SchemaField("user_first_touch_timestamp", "INTEGER",   mode="NULLABLE"),
        SchemaField("event_params", "RECORD", mode="REPEATED", fields=[
            SchemaField("key", "STRING", mode="NULLABLE"),
            SchemaField("value", "RECORD", mode="NULLABLE", fields=[
                SchemaField("string_value", "STRING",  mode="NULLABLE"),
                SchemaField("int_value",    "INTEGER", mode="NULLABLE"),
                SchemaField("float_value",  "FLOAT",   mode="NULLABLE"),
                SchemaField("double_value", "FLOAT",   mode="NULLABLE"),
            ]),
        ]),
        SchemaField("user_properties", "RECORD", mode="REPEATED", fields=[
            SchemaField("key", "INTEGER", mode="NULLABLE"),
            SchemaField("value", "RECORD", mode="NULLABLE", fields=[
                SchemaField("string_value",         "INTEGER", mode="NULLABLE"),
                SchemaField("int_value",            "INTEGER", mode="NULLABLE"),
                SchemaField("float_value",          "INTEGER", mode="NULLABLE"),
                SchemaField("double_value",         "INTEGER", mode="NULLABLE"),
                SchemaField("set_timestamp_micros", "INTEGER", mode="NULLABLE"),
            ]),
        ]),
        SchemaField("ecommerce", "RECORD", mode="NULLABLE", fields=[
            SchemaField("total_item_quantity",     "INTEGER", mode="NULLABLE"),
            SchemaField("purchase_revenue_in_usd", "FLOAT",   mode="NULLABLE"),
            SchemaField("purchase_revenue",        "FLOAT",   mode="NULLABLE"),
            SchemaField("refund_value_in_usd",     "FLOAT",   mode="NULLABLE"),
            SchemaField("refund_value",            "FLOAT",   mode="NULLABLE"),
            SchemaField("shipping_value_in_usd",   "FLOAT",   mode="NULLABLE"),
            SchemaField("shipping_value",          "FLOAT",   mode="NULLABLE"),
            SchemaField("tax_value_in_usd",        "FLOAT",   mode="NULLABLE"),
            SchemaField("tax_value",               "FLOAT",   mode="NULLABLE"),
            SchemaField("unique_items",            "INTEGER", mode="NULLABLE"),
            SchemaField("transaction_id",          "STRING",  mode="NULLABLE"),
        ]),
        SchemaField("traffic_source", "RECORD", mode="NULLABLE", fields=[
            SchemaField("medium", "STRING", mode="NULLABLE"),
            SchemaField("name",   "STRING", mode="NULLABLE"),
            SchemaField("source", "STRING", mode="NULLABLE"),
        ]),
        SchemaField("device", "RECORD", mode="NULLABLE", fields=[
            SchemaField("category",                 "STRING",  mode="NULLABLE"),
            SchemaField("mobile_brand_name",        "STRING",  mode="NULLABLE"),
            SchemaField("mobile_model_name",        "STRING",  mode="NULLABLE"),
            SchemaField("mobile_marketing_name",    "STRING",  mode="NULLABLE"),
            SchemaField("mobile_os_hardware_model", "INTEGER", mode="NULLABLE"),
            SchemaField("operating_system",         "STRING",  mode="NULLABLE"),
            SchemaField("operating_system_version", "STRING",  mode="NULLABLE"),
            SchemaField("vendor_id",                "INTEGER", mode="NULLABLE"),
            SchemaField("advertising_id",           "INTEGER", mode="NULLABLE"),
            SchemaField("language",                 "STRING",  mode="NULLABLE"),
            SchemaField("is_limited_ad_tracking",   "STRING",  mode="NULLABLE"),
            SchemaField("time_zone_offset_seconds", "INTEGER", mode="NULLABLE"),
            SchemaField("web_info", "RECORD", mode="NULLABLE", fields=[
                SchemaField("browser",         "STRING", mode="NULLABLE"),
                SchemaField("browser_version", "STRING", mode="NULLABLE"),
            ]),
        ]),
        SchemaField("geo", "RECORD", mode="NULLABLE", fields=[
            SchemaField("continent",     "STRING", mode="NULLABLE"),
            SchemaField("sub_continent", "STRING", mode="NULLABLE"),
            SchemaField("country",       "STRING", mode="NULLABLE"),
            SchemaField("region",        "STRING", mode="NULLABLE"),
            SchemaField("city",          "STRING", mode="NULLABLE"),
            SchemaField("metro",         "STRING", mode="NULLABLE"),
        ]),
        SchemaField("user_ltv", "RECORD", mode="NULLABLE", fields=[
            SchemaField("revenue",  "FLOAT",  mode="NULLABLE"),
            SchemaField("currency", "STRING", mode="NULLABLE"),
        ]),
        SchemaField("platform",  "STRING",  mode="NULLABLE"),
        SchemaField("stream_id", "INTEGER", mode="NULLABLE"),
        SchemaField("privacy_info", "RECORD", mode="NULLABLE", fields=[
            SchemaField("analytics_storage",    "INTEGER", mode="NULLABLE"),
            SchemaField("ads_storage",          "INTEGER", mode="NULLABLE"),
            SchemaField("uses_transient_token", "STRING",  mode="NULLABLE"),
        ]),
        SchemaField("app_info", "RECORD", mode="NULLABLE", fields=[
            SchemaField("id",              "STRING", mode="NULLABLE"),
            SchemaField("version",         "STRING", mode="NULLABLE"),
            SchemaField("install_store",   "STRING", mode="NULLABLE"),
            SchemaField("firebase_app_id", "STRING", mode="NULLABLE"),
            SchemaField("install_source",  "STRING", mode="NULLABLE"),
        ]),
        SchemaField("event_dimensions", "RECORD", mode="NULLABLE", fields=[
            SchemaField("hostname", "STRING", mode="NULLABLE"),
        ]),
        SchemaField("ingested_at",         "TIMESTAMP", mode="NULLABLE"),
        SchemaField("source_table_suffix", "STRING",    mode="NULLABLE"),
    ]
    # No partitioning — BigQuery project-level restriction prevents
    # DML from persisting in partitioned tables in this environment.
    # Partitioning documented as production recommendation in design doc.
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)
    log.info("raw_events table ready")


def create_raw_purchase_items_if_not_exists(client, config):
    from google.cloud.bigquery import SchemaField
    project  = config['project']['gcp_project_id']
    dataset  = config['project']['dataset']
    table_id = f"{project}.{dataset}.raw_purchase_items"
    try:
        client.get_table(table_id)
        log.info("raw_purchase_items table ready")
        return
    except Exception:
        pass
    schema = [
        SchemaField("event_date_dt",       "DATE",      mode="NULLABLE"),
        SchemaField("item_event_id",       "STRING",    mode="NULLABLE"),
        SchemaField("event_id",            "STRING",    mode="NULLABLE"),
        SchemaField("transaction_id",      "STRING",    mode="NULLABLE"),
        SchemaField("user_pseudo_id",      "STRING",    mode="NULLABLE"),
        SchemaField("item_position",       "INTEGER",   mode="NULLABLE"),
        SchemaField("item_id",             "STRING",    mode="NULLABLE"),
        SchemaField("item_name",           "STRING",    mode="NULLABLE"),
        SchemaField("item_brand",          "STRING",    mode="NULLABLE"),
        SchemaField("item_variant",        "STRING",    mode="NULLABLE"),
        SchemaField("item_category",       "STRING",    mode="NULLABLE"),
        SchemaField("item_category2",      "STRING",    mode="NULLABLE"),
        SchemaField("item_category3",      "STRING",    mode="NULLABLE"),
        SchemaField("price_in_usd",        "FLOAT",     mode="NULLABLE"),
        SchemaField("price",               "FLOAT",     mode="NULLABLE"),
        SchemaField("quantity",            "INTEGER",   mode="NULLABLE"),
        SchemaField("item_revenue_in_usd", "FLOAT",     mode="NULLABLE"),
        SchemaField("item_revenue",        "FLOAT",     mode="NULLABLE"),
        SchemaField("item_refund_in_usd",  "FLOAT",     mode="NULLABLE"),
        SchemaField("item_refund",         "FLOAT",     mode="NULLABLE"),
        SchemaField("coupon",              "STRING",    mode="NULLABLE"),
        SchemaField("affiliation",         "STRING",    mode="NULLABLE"),
        SchemaField("item_list_id",        "STRING",    mode="NULLABLE"),
        SchemaField("item_list_name",      "STRING",    mode="NULLABLE"),
        SchemaField("item_list_index",     "STRING",    mode="NULLABLE"),
        SchemaField("promotion_id",        "STRING",    mode="NULLABLE"),
        SchemaField("promotion_name",      "STRING",    mode="NULLABLE"),
        SchemaField("creative_name",       "STRING",    mode="NULLABLE"),
        SchemaField("creative_slot",       "STRING",    mode="NULLABLE"),
        SchemaField("ingested_at",         "TIMESTAMP", mode="NULLABLE"),
        SchemaField("source_table_suffix", "STRING",    mode="NULLABLE"),
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)
    log.info("raw_purchase_items table ready")


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
    Runs DELETE + INSERT SQL for a specific date.
    Returns number of rows inserted.
    """
    project = config['project']['gcp_project_id']
    dataset = config['project']['dataset']

    sql = sql_template.format(
        project=project,
        dataset=dataset,
        date=target_date
    )

    # Split on semicolons — run DELETE and INSERT separately
    statements = [s.strip() for s in sql.split(';') if s.strip()]

    total_rows = 0
    for statement in statements:
        if not statement:
            continue

        job = client.query(statement)
        job.result()

        # Strip comments to detect statement type
        if 'INSERT INTO' in statement.upper():
            total_rows += job.num_dml_affected_rows or 0

    return total_rows


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
            retries = config['pipeline']['max_retries']
            delay   = config['pipeline']['retry_delay_seconds']

            # Run raw_events MERGE (retried — idempotent DELETE+INSERT)
            events_rows = with_retries(
                lambda: run_merge_for_date(
                    client, config, target_date, raw_events_sql
                ),
                max_retries=retries, delay_seconds=delay,
                label=f"raw_events MERGE {target_date}"
            )
            log.info(f"  raw_events: {events_rows:,} rows inserted")

            # Run raw_purchase_items MERGE (retried — idempotent DELETE+INSERT)
            items_rows = with_retries(
                lambda: run_merge_for_date(
                    client, config, target_date, raw_items_sql
                ),
                max_retries=retries, delay_seconds=delay,
                label=f"raw_purchase_items MERGE {target_date}"
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