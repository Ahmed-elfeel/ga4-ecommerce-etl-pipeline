"""
sheets.py
=========
Exports mart metrics from BigQuery to Google Sheets.

Tabs:
- Tab 1: Daily Metrics  (mart_daily_metrics)
- Tab 2: Weekly Metrics (mart_weekly_metrics)

Write pattern:
- Atomic overwrite: clear → write
- Dates never duplicate — full overwrite on each run
- Headers always included as first row

Usage:
    python python/sheets.py
"""

import logging
from datetime import datetime, timezone

import yaml
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ─────────────────────────────────────────────
# LOGGING
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
# CLIENTS
# ─────────────────────────────────────────────
def get_bq_client(config: dict) -> bigquery.Client:
    """Initialize BigQuery client."""
    credentials = service_account.Credentials.from_service_account_file(
        config['credentials']['service_account_path'],
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(
        credentials=credentials,
        project=config['project']['gcp_project_id']
    )


def get_sheets_service(config: dict):
    """Initialize Google Sheets API service."""
    credentials = service_account.Credentials.from_service_account_file(
        config['credentials']['service_account_path'],
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    return build('sheets', 'v4', credentials=credentials)


# ─────────────────────────────────────────────
# BIGQUERY — READ MART TABLES
# ─────────────────────────────────────────────
def read_daily_metrics(client: bigquery.Client, config: dict) -> list:
    """
    Read mart_daily_metrics from BigQuery.
    Returns list of rows as lists (for Sheets API).
    First row is headers.
    """
    project = config['project']['gcp_project_id']

    query = f"""
        SELECT
            CAST(date AS STRING)        AS date,
            gross_revenue,
            refund_amount,
            net_revenue,
            total_orders,
            avg_order_value,
            unique_customers,
            new_customers,
            returning_customers,
            sessions,
            ROUND(conversion_rate * 100, 2) AS conversion_rate_pct
        FROM `{project}.darkroom_ecommerce_mart.mart_daily_metrics`
        ORDER BY date ASC
    """

    rows = list(client.query(query).result())
    log.info(f"Read {len(rows)} rows from mart_daily_metrics")

    headers = [
        "Date",
        "Gross Revenue (USD)",
        "Refund Amount (USD)",
        "Net Revenue (USD)",
        "Total Orders",
        "Avg Order Value (USD)",
        "Unique Customers",
        "New Customers",
        "Returning Customers",
        "Sessions",
        "Conversion Rate (%)"
    ]

    data = [headers]
    for row in rows:
        data.append([
            str(row.date),
            float(row.gross_revenue),
            float(row.refund_amount),
            float(row.net_revenue),
            int(row.total_orders),
            float(row.avg_order_value),
            int(row.unique_customers),
            int(row.new_customers),
            int(row.returning_customers),
            int(row.sessions),
            float(row.conversion_rate_pct)
        ])



    return data


def read_weekly_metrics(client: bigquery.Client, config: dict) -> list:
    """
    Read mart_weekly_metrics from BigQuery.
    Returns list of rows as lists (for Sheets API).
    First row is headers.
    """
    project = config['project']['gcp_project_id']

    query = f"""
        SELECT
            CAST(week_start AS STRING)  AS week_start,
            CAST(week_end AS STRING)    AS week_end,
            gross_revenue,
            refund_amount,
            net_revenue,
            total_orders,
            avg_order_value,
            new_customers,
            returning_customers,
            sessions,
            ROUND(conversion_rate * 100, 2) AS conversion_rate_pct
        FROM `{project}.darkroom_ecommerce_mart.mart_weekly_metrics`
        ORDER BY week_start ASC
    """

    rows = list(client.query(query).result())
    log.info(f"Read {len(rows)} rows from mart_weekly_metrics")

    headers = [
        "Week Start (Monday)",
        "Week End (Sunday)",
        "Gross Revenue (USD)",
        "Refund Amount (USD)",
        "Net Revenue (USD)",
        "Total Orders",
        "Avg Order Value (USD)",
        "New Customers",
        "Returning Customers",
        "Sessions",
        "Conversion Rate (%)"
    ]

    data = [headers]
    for row in rows:
        data.append([
            str(row.week_start),
            str(row.week_end),
            float(row.gross_revenue or 0),
            float(row.refund_amount or 0),
            float(row.net_revenue or 0),
            int(row.total_orders or 0),
            float(row.avg_order_value or 0),
            int(row.new_customers or 0),
            int(row.returning_customers or 0),
            int(row.sessions or 0),
            float(row.conversion_rate_pct or 0)
        ])

    return data


# ─────────────────────────────────────────────
# SHEETS — WRITE
# ─────────────────────────────────────────────
def write_to_sheet(
    service,
    spreadsheet_id: str,
    tab_name: str,
    data: list
) -> None:
    """
    Writes data to a Google Sheets tab.

    Pattern: clear → write
    - Clear removes all existing data including old dates
    - Write inserts fresh data with headers
    - Atomic enough for reporting use case — full overwrite
    - Dates never duplicate because we always clear first
    """
    sheets = service.spreadsheets()

    # Step 1: Clear existing content
    sheets.values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1:Z10000"
    ).execute()
    log.info(f"Cleared tab: {tab_name}")

    # Step 2: Write fresh data
    sheets.values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{tab_name}!A1",
        valueInputOption="USER_ENTERED",
        body={"values": data}
    ).execute()

    log.info(f"Written {len(data) - 1} rows to tab: {tab_name}")


def format_sheet(
    service,
    spreadsheet_id: str,
    sheet_id: int,
    num_columns: int
) -> None:
    """
    Applies basic formatting to a sheet tab:
    - Bold header row
    - Freeze header row
    - Auto-resize columns
    """
    requests = [
    # Bold header row
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": 1
                },
                "cell": {
                    "userEnteredFormat": {
                        "textFormat": {"bold": True}
                    }
                },
                "fields": "userEnteredFormat.textFormat.bold"
            }
        },
        # Freeze header row
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {"frozenRowCount": 1}
                },
                "fields": "gridProperties.frozenRowCount"
            }
        },
        # Auto-resize all columns
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": num_columns
                }
            }
        }
    ]

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests}
    ).execute()
    log.info(f"Formatted sheet_id: {sheet_id}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def export_to_sheets(config: dict) -> None:
    """
    Main export function.
    Reads from BigQuery mart tables and writes to Google Sheets.
    """
    spreadsheet_id = config['sheets']['spreadsheet_id']

    bq_client     = get_bq_client(config)
    sheets_service = get_sheets_service(config)

    # Get sheet IDs for formatting
    sheet_meta = sheets_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id
    ).execute()

    sheet_ids = {
        s['properties']['title']: s['properties']['sheetId']
        for s in sheet_meta['sheets']
    }
    log.info(f"Found tabs: {list(sheet_ids.keys())}")

    # ── Daily Metrics ──────────────────────────
    log.info("Exporting daily metrics...")
    daily_data = read_daily_metrics(bq_client, config)
    write_to_sheet(
        sheets_service,
        spreadsheet_id,
        "Daily Metrics",
        daily_data
    )
    format_sheet(
        sheets_service,
        spreadsheet_id,
        sheet_ids["Daily Metrics"],
        num_columns=len(daily_data[0])
    )

    # ── Weekly Metrics ─────────────────────────
    log.info("Exporting weekly metrics...")
    weekly_data = read_weekly_metrics(bq_client, config)
    write_to_sheet(
        sheets_service,
        spreadsheet_id,
        "Weekly Metrics",
        weekly_data
    )
    format_sheet(
        sheets_service,
        spreadsheet_id,
        sheet_ids["Weekly Metrics"],
        num_columns=len(weekly_data[0])
    )

    log.info(
        f"Export complete: "
        f"{len(daily_data)-1} daily rows, "
        f"{len(weekly_data)-1} weekly rows"
    )


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    config = load_config()
    export_to_sheets(config)
