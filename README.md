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
Tab 1: Daily Metrics | Tab 2: Weekly Metrics

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
| Total Orders | COUNT DISTINCT transaction_id |
| Avg Order Value | Gross Revenue / Total Orders |
| Unique Customers | COUNT DISTINCT user_pseudo_id on purchase events |
| New Customers | Users whose first purchase is on this date |
| Returning Customers | Unique Customers − New Customers |
| Sessions | COUNT DISTINCT session_key from session_start events |
| Conversion Rate | Total Orders / Sessions |

### Weekly Metrics (Tab 2)
Same metrics aggregated by ISO week (Monday–Sunday).

## Data Quality

- 18 dbt tests on every run (unique + not_null on all primary keys)
- All 92 dates validated against source — row counts and revenue match exactly
- pipeline_runs table tracks every run with status, rows processed, and duration

## Google Sheets Output

[GA4 Ecommerce ETL Pipeline](https://docs.google.com/spreadsheets/d/1JBQIF8Qq2b6rPFioHVJAjI7gka5Bxw3jzK3EsBOD50Y)

## Production Design

See [docs/production_design.md](docs/production_design.md) for full production architecture using Cloud Run Jobs + Cloud Scheduler.

## Known Data Characteristics

- **2020-11-01:** Zero purchase metrics — all purchase events have NULL transaction_id (obfuscated dataset)
- **first_seen_date:** NULL for ~30 purchase-only users with no session_start events (expected)