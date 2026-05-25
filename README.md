# GA4 Ecommerce ETL Pipeline

End-to-end data pipeline that extracts GA4 ecommerce event data, transforms it into reporting metrics, and delivers daily and weekly reports to Google Sheets.

## Dataset

Google Merchandise Store GA4 ecommerce data (Nov 2020 – Jan 2021):
`bigquery-public-data.ga4_obfuscated_sample_ecommerce`

## Architecture
Source (BigQuery Public)
↓ extract.py
Raw Layer (BigQuery)
raw_events, raw_purchase_items, pipeline_runs
↓ dbt staging
Staging Layer (BigQuery)
stg_sessions, stg_purchases, stg_purchase_items, stg_users
↓ dbt mart
Mart Layer (BigQuery)
mart_daily_metrics, mart_weekly_metrics, mart_product_metrics
↓ sheets.py
Google Sheets
Tab 1: Daily Metrics | Tab 2: Weekly Metrics | Tab 3: Top Products | Tab 4: Documentation

## Project Structure
ga4-ecommerce-etl-pipeline/
├── config/
│   └── config.yaml              # Project configuration
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── sources.yml
│   │   ├── staging/             # 4 staging models + schema.yml
│   │   └── mart/                # 3 mart models + schema.yml
│   └── profiles.yml             # BigQuery connection (gitignored)
├── docs/
│   └── production_design.md     # Production architecture design
├── python/
│   ├── extract.py               # Raw layer extraction
│   ├── sheets.py                # Google Sheets export
│   └── pipeline.py              # End-to-end orchestrator
├── sql/
│   └── raw/
│       ├── raw_events.sql       # Raw events INSERT
│       └── raw_purchase_items.sql  # Raw purchase items INSERT
├── tests/
│   └── test_connection.py       # BigQuery connection test
└── requirements.txt

## Setup

### Prerequisites

- Python 3.11+
- GCP project with BigQuery, Sheets API, Drive API enabled
- Service account with BigQuery Admin + Sheets Editor roles
- dbt-bigquery installed

### Install dependencies

```bash
pip install -r requirements.txt
```

### Configure

1. Copy service account JSON to `config/credentials/service_account.json`
2. Update `config/config.yaml` with your GCP project ID and Sheet ID
3. Create `dbt/profiles.yml` with BigQuery connection

### Run

**Full pipeline:**
```bash
python python/pipeline.py
```

**Specific date only:**
```bash
python python/pipeline.py --date 20201101
```

**Individual steps:**
```bash
python python/extract.py          # Extract raw data
cd dbt && dbt run                 # Transform
cd dbt && dbt test                # Test
python python/sheets.py           # Export to Sheets
```

## Metrics

### Daily Metrics (Tab 1)
| Metric | Definition |
|---|---|
| Gross Revenue | SUM of purchase_revenue_in_usd |
| Refund Amount | SUM of refund_value_in_usd |
| Net Revenue | Gross Revenue − Refund Amount |
| Total Orders | COUNT DISTINCT event_id (purchase event grain — see Data Quality) |
| Avg Order Value | Gross Revenue / Total Orders |
| Unique Customers | COUNT DISTINCT user_pseudo_id on purchase events |
| New Customers | Users whose first purchase is on this date |
| Returning Customers | Unique Customers − New Customers |
| Sessions | COUNT DISTINCT session_key from session_start events |
| Conversion Rate | Total Orders / Sessions |

### Weekly Metrics (Tab 2)
Same metrics aggregated by ISO week (Monday–Sunday). Unique Customers and Returning Customers are recomputed at week grain — not summed from daily.

### Top Products (Tab 3)
Top 20 products by revenue over the full period: revenue rank, product name, category, total/refund/net revenue, quantity sold, orders, average price, and quantity rank.

### Documentation (Tab 4)
Business-friendly reference for all metrics across the three data tabs. Three columns — **Metric**, **What It Measures**, **How It's Calculated** — plus a Dataset Notes section covering date range, known data caveats, and the week definition. Intended for stakeholders who need to interpret the numbers without technical context.

## Data Quality

- Structural tests on every run (unique + not_null on all primary keys) plus business-logic reconciliation tests
- All 92 dates validated against source — row counts and revenue match exactly
- pipeline_runs table tracks every run with status, rows processed, and duration

## Google Sheets Output

[GA4 Ecommerce ETL Pipeline](https://docs.google.com/spreadsheets/d/1JBQIF8Qq2b6rPFioHVJAjI7gka5Bxw3jzK3EsBOD50Y)

## Production Design

See [docs/production_design.md](docs/production_design.md) for full production architecture using Cloud Run Jobs + Cloud Scheduler.

## Data Quality & Caveats

- **Order grain:** `transaction_id` is not unique per order in this obfuscated
  dataset (distinct purchases share ids), so orders are counted at the purchase
  event grain (`event_id`). This is the reliable unit and equals `transaction_id`
  counts whenever ids are clean.
- **Weekly customer metrics are non-additive.** `unique_customers` and
  `returning_customers` are recomputed at week grain from source, not summed from
  daily — a customer active on multiple days in a week is counted once. Revenue,
  orders, and sessions remain additive and reconcile to the daily tab exactly.
- **2020-11-01:** zero purchase metrics — all purchase events that day have NULL
  `transaction_id` (obfuscation artifact), so they are excluded from orders/revenue.
- **first_seen_date:** NULL for ~30 purchase-only users with no session_start events (expected)