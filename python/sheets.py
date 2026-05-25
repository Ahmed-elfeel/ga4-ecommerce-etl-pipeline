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
import time
from datetime import datetime, timezone

import yaml
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build
import argparse

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
# RETRY HELPER
# ─────────────────────────────────────────────
def with_retries(fn, max_retries=3, delay_seconds=30, backoff=2.0, label=""):
    """Retry an idempotent operation with exponential backoff.
    Safe here because all retried ops (clear+write) are idempotent.
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
            unique_customers,
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
        "Unique Customers",
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
            int(row.unique_customers or 0),
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
    max_retries = 3
    delay       = 30

    def _clear():
        sheets.values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1:Z10000"
        ).execute()

    def _write():
        sheets.values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{tab_name}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": data}
        ).execute()

    # Step 1: Clear existing content (retried — idempotent)
    with_retries(_clear, max_retries=max_retries, delay_seconds=delay,
                 label=f"clear {tab_name}")
    log.info(f"Cleared tab: {tab_name}")

    # Step 2: Write fresh data (retried — idempotent after clear)
    with_retries(_write, max_retries=max_retries, delay_seconds=delay,
                 label=f"write {tab_name}")
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
# SHEETS — TAB MANAGEMENT
# ─────────────────────────────────────────────
def ensure_tab_exists(service, spreadsheet_id: str, tab_name: str) -> None:
    """Create the tab if it doesn't already exist."""
    meta = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    titles = [s['properties']['title'] for s in meta['sheets']]
    if tab_name not in titles:
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
        ).execute()
        log.info(f"Created tab: {tab_name}")


# ─────────────────────────────────────────────
# BIGQUERY — READ PRODUCT METRICS
# ─────────────────────────────────────────────
def read_product_metrics(client: bigquery.Client, config: dict) -> list:
    """
    Read mart_product_metrics from BigQuery (top 20 by revenue).
    Returns list of rows as lists (for Sheets API).
    First row is headers.
    """
    project = config['project']['gcp_project_id']
    query = f"""
        SELECT
            revenue_rank,
            item_name,
            category,
            total_revenue,
            total_refunds,
            net_revenue,
            total_quantity_sold,
            total_orders,
            avg_price_usd,
            quantity_rank
        FROM `{project}.darkroom_ecommerce_mart.mart_product_metrics`
        ORDER BY revenue_rank ASC
        LIMIT 20
    """
    rows = list(client.query(query).result())
    log.info(f"Read {len(rows)} rows from mart_product_metrics")
    headers = [
        "Revenue Rank", "Product", "Category", "Total Revenue (USD)",
        "Total Refunds (USD)", "Net Revenue (USD)", "Quantity Sold",
        "Orders", "Avg Price (USD)", "Quantity Rank"
    ]
    data = [headers]
    for row in rows:
        data.append([
            int(row.revenue_rank or 0), str(row.item_name or ""),
            str(row.category or ""), float(row.total_revenue or 0),
            float(row.total_refunds or 0), float(row.net_revenue or 0),
            int(row.total_quantity_sold or 0), int(row.total_orders or 0),
            float(row.avg_price_usd or 0), int(row.quantity_rank or 0)
        ])
    return data


# ─────────────────────────────────────────────
# DOCUMENTATION TAB
# ─────────────────────────────────────────────
def build_documentation_data() -> tuple:
    """
    Build the content for the Documentation tab.

    Written for business stakeholders — no technical jargon.
    Returns (rows, section_row_indices) where section_row_indices
    are the 0-based row numbers that should be formatted as section headers.
    """
    rows = []
    section_indices = []

    def col(name, what, how):
        rows.append([name, what, how])

    def section(title):
        section_indices.append(len(rows))
        rows.append([title, "", ""])

    def spacer():
        rows.append(["", "", ""])

    # Column headers
    col("Metric", "What It Measures", "How It's Calculated")
    spacer()

    # ── Daily Metrics ──────────────────────────────────────────────
    section("DAILY METRICS  —  Tab 1")
    col("Date",
        "The calendar day the data represents",
        "Each row covers one day of store activity, from November 1, 2020 through January 31, 2021")
    col("Gross Revenue (USD)",
        "Total sales income collected before deducting any refunds",
        "All purchase amounts on that day added together")
    col("Refund Amount (USD)",
        "Total value of refunds paid back to customers that day",
        "All refund amounts on that day added together")
    col("Net Revenue (USD)",
        "The revenue the store actually kept after refunds were deducted",
        "Gross Revenue minus Refund Amount")
    col("Total Orders",
        "Number of completed purchases on that day",
        "Each purchase transaction counts as one order")
    col("Avg Order Value (USD)",
        "The average amount customers spent per order",
        "Gross Revenue divided by Total Orders")
    col("Unique Customers",
        "How many different customers made at least one purchase",
        "Each shopper counted once, regardless of how many orders they placed that day")
    col("New Customers",
        "Shoppers who were buying from the store for the very first time",
        "Customers whose very first purchase in the entire dataset was on this day")
    col("Returning Customers",
        "Customers who had already bought from the store before",
        "Unique Customers minus New Customers")
    col("Sessions",
        "Number of individual website visits",
        "Each time someone browses the store counts as one session")
    col("Conversion Rate (%)",
        "The percentage of website visits that ended in a completed purchase",
        "Orders divided by Sessions, multiplied by 100. A value of 2.34 means 2.34% of visitors made a purchase")
    spacer()

    # ── Weekly Metrics ─────────────────────────────────────────────
    section("WEEKLY METRICS  —  Tab 2")
    col("Week Start (Monday)",
        "The Monday that opens the reporting week",
        "Weeks follow the standard business calendar: Monday through Sunday")
    col("Week End (Sunday)",
        "The Sunday that closes the reporting week",
        "Always 6 days after the Week Start")
    col("Gross Revenue (USD)",
        "Total sales income for the full week before refunds",
        "Each day's Gross Revenue added together across all 7 days in the week")
    col("Refund Amount (USD)",
        "Total refunds issued during the week",
        "Each day's Refund Amount added together across the week")
    col("Net Revenue (USD)",
        "Revenue the store kept for the week after all refunds",
        "Weekly Gross Revenue minus Weekly Refund Amount")
    col("Total Orders",
        "Total purchases placed during the week",
        "Each day's Total Orders added together across the week")
    col("Avg Order Value (USD)",
        "Average amount spent per order during the week",
        "Weekly Gross Revenue divided by Weekly Total Orders")
    col("Unique Customers",
        "How many different customers made at least one purchase during the week",
        "Each shopper counted once for the whole week — a customer who bought on Monday and again on Thursday is still one customer, not two. This figure is calculated directly from the full week's purchases, not by adding up the daily numbers")
    col("New Customers",
        "First-time buyers during the week",
        "Customers whose very first purchase ever fell within this week. These are additive — the weekly total equals the sum of the daily New Customer counts for the same days")
    col("Returning Customers",
        "Customers who had made at least one purchase before this week",
        "Weekly Unique Customers minus Weekly New Customers")
    col("Sessions",
        "Total website visits during the week",
        "Each day's Sessions added together across the week")
    col("Conversion Rate (%)",
        "Percentage of the week's website visits that resulted in a purchase",
        "Weekly Total Orders divided by Weekly Sessions, multiplied by 100")
    spacer()

    # ── Top Products ───────────────────────────────────────────────
    section("TOP PRODUCTS  —  Tab 3  (top 20 by revenue over the full period)")
    col("Revenue Rank",
        "This product's position when all products are sorted from highest to lowest total revenue",
        "1 = the product that earned the most revenue across the full Nov 2020 – Jan 2021 period")
    col("Product",
        "The name of the product",
        "Product name as recorded at the point of sale")
    col("Category",
        "The product family it belongs to",
        "Primary category assigned to the product in the store catalogue")
    col("Total Revenue (USD)",
        "Total sales income this product generated, before refunds",
        "All purchase amounts for this product added together across the full period")
    col("Total Refunds (USD)",
        "Total value of refunds on this product across the full period",
        "All refund amounts for this product added together")
    col("Net Revenue (USD)",
        "Revenue from this product after all refunds are deducted",
        "Total Revenue minus Total Refunds")
    col("Quantity Sold",
        "Total number of units of this product purchased",
        "All quantities across every order that included this product, added together")
    col("Orders",
        "Number of orders that contained this product",
        "Count of distinct purchase transactions that included at least one unit of this product")
    col("Avg Price (USD)",
        "The average price paid per unit",
        "Total Revenue divided by Quantity Sold")
    col("Quantity Rank",
        "This product's position when all products are sorted from most to least units sold",
        "1 = the best-selling product by volume across the full period")
    spacer()

    # ── Dataset Notes ──────────────────────────────────────────────
    section("DATASET NOTES")
    col("Date range",
        "This report covers November 1, 2020 through January 31, 2021 — 92 days in total",
        "Source: Google Analytics 4 sample ecommerce data from the Google Merchandise Store (public dataset)")
    col("November 1, 2020",
        "This day shows zero orders and zero revenue",
        "A known characteristic of the source data: order identifiers are missing for all purchases on this specific day, so they cannot be included in order or revenue counts. Website session data for that day is still present")
    col("Week definition",
        "All weeks in this report run Monday through Sunday",
        "The international business-week standard (ISO 8601) is used. Note: Google Analytics natively displays Sunday–Saturday weeks, so week-level totals here will differ from what you see when viewing the same period directly in GA4")
    col("Unique Customers (weekly vs. daily)",
        "Weekly unique customer counts cannot be obtained by adding up the daily figures",
        "A shopper who buys on Monday and Thursday in the same week appears as 2 unique customers in the daily tab but as 1 in the weekly tab. Revenue, orders, and sessions do add up cleanly between the two tabs")

    return rows, section_indices


def format_documentation_sheet(
    service,
    spreadsheet_id: str,
    sheet_id: int,
    section_indices: list
) -> None:
    """
    Formats the Documentation tab:
    - Dark header row (row 0) — bold white text on dark background
    - Light blue-grey section header rows — bold
    - Freeze header row
    - Auto-resize all 3 columns
    """
    requests = [
        # Header row — dark background, white bold text
        {
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": 1,
                    "startColumnIndex": 0, "endColumnIndex": 3
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.18, "green": 0.34, "blue": 0.52},
                        "textFormat": {
                            "bold": True,
                            "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0}
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat)"
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
        # Auto-resize all 3 columns
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": 3
                }
            }
        }
    ]

    # Section header rows — light blue-grey background, bold
    for row_idx in section_indices:
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row_idx, "endRowIndex": row_idx + 1,
                    "startColumnIndex": 0, "endColumnIndex": 3
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.82, "green": 0.88, "blue": 0.95},
                        "textFormat": {"bold": True}
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat.bold)"
            }
        })

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": requests}
    ).execute()
    log.info(f"Formatted documentation sheet_id: {sheet_id}")


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

    # ── Top Products ───────────────────────────
    products_tab = config['sheets']['products_tab']
    ensure_tab_exists(sheets_service, spreadsheet_id, products_tab)
    # Re-fetch sheet IDs so the new tab is included for formatting
    sheet_meta = sheets_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id
    ).execute()
    sheet_ids = {
        s['properties']['title']: s['properties']['sheetId']
        for s in sheet_meta['sheets']
    }

    log.info("Exporting product metrics...")
    product_data = read_product_metrics(bq_client, config)
    write_to_sheet(sheets_service, spreadsheet_id, products_tab, product_data)
    format_sheet(sheets_service, spreadsheet_id, sheet_ids[products_tab],
                 num_columns=len(product_data[0]))

    # ── Documentation ──────────────────────────
    ensure_tab_exists(sheets_service, spreadsheet_id, "Documentation")
    # Re-fetch sheet IDs to pick up any newly created tabs
    sheet_meta = sheets_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id
    ).execute()
    sheet_ids = {
        s['properties']['title']: s['properties']['sheetId']
        for s in sheet_meta['sheets']
    }

    log.info("Writing documentation tab...")
    doc_data, section_indices = build_documentation_data()
    write_to_sheet(sheets_service, spreadsheet_id, "Documentation", doc_data)
    format_documentation_sheet(
        sheets_service, spreadsheet_id,
        sheet_ids["Documentation"], section_indices
    )

    log.info(
        f"Export complete: "
        f"{len(daily_data)-1} daily rows, "
        f"{len(weekly_data)-1} weekly rows, "
        f"{len(product_data)-1} product rows, "
        f"documentation tab written"
    )


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Export mart metrics to Google Sheets'
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to config file'
    )
    args = parser.parse_args()
    config = load_config(args.config)
    export_to_sheets(config)
