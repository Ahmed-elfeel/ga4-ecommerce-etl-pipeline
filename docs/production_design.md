# Production Pipeline Design

## Overview

This document describes how the GA4 Ecommerce ETL pipeline would run automatically in production using GCP services. The pipeline extracts GA4 event data, transforms it through a medallion architecture, and delivers daily and weekly metrics to Google Sheets.

---

## Architecture
Cloud Scheduler (daily 6am UTC)
↓
Cloud Run Jobs (pipeline.py)
↓
┌─────────────────────────────────┐
│  Step 1: extract.py             │  Raw layer — BigQuery
│  Step 2: dbt run staging        │  Staging layer — BigQuery
│  Step 3: dbt run mart           │  Mart layer — BigQuery
│  Step 4: dbt test               │  Quality gate
│  Step 5: sheets.py              │  Google Sheets export
└─────────────────────────────────┘
↓
Google Sheets (Daily + Weekly tabs)
↓
Cloud Logging + Cloud Monitoring

---

## GCP Services

| Service | Role |
|---|---|
| BigQuery | Data warehouse — raw, staging, mart layers |
| Cloud Run Jobs | Serverless pipeline execution |
| Cloud Scheduler | Trigger Cloud Run Job daily at 6am UTC |
| Cloud Storage | Store dbt artifacts, pipeline logs, Docker image |
| Secret Manager | Store service account credentials securely |
| Cloud Logging | Centralized log aggregation |
| Cloud Monitoring | Alerting on pipeline failures |
| Google Sheets API | Export mart metrics to stakeholders |

**Why Cloud Run Jobs over Cloud Composer:**
Cloud Composer (managed Airflow) is appropriate for enterprise-scale orchestration with complex DAG dependencies across many teams. For this pipeline's scale — one sequential job running daily — Cloud Run Jobs + Cloud Scheduler has significantly lower operational overhead, no cluster to manage, and pay-per-execution pricing. The pipeline.py orchestrator handles step sequencing natively.

---

## Scheduling

**Schedule:** Daily at 6am UTC (covers previous full day)

**Why 6am UTC:**
- GA4 data for the previous day is typically complete by midnight UTC
- 6am provides a 6-hour buffer for any late-arriving events
- Stakeholders in US timezones (11pm PT / 1am ET) have fresh data ready at start of business day

**Cloud Scheduler → Cloud Run Jobs:**
Cloud Scheduler triggers Cloud Run Job at 0 6 * * *
Cloud Run Job executes: python pipeline.py
pipeline.py orchestrates all 5 steps sequentially
Exit code 0 = success, Exit code 1 = failure
Cloud Monitoring alerts on non-zero exit code

---

## Incremental Loading Strategy

The pipeline uses a smart date discovery pattern rather than a fixed rolling window:

**Step 1 — Missing dates:**
completed_dates = pipeline_runs WHERE status = 'success'
missing_dates   = source_dates - completed_dates

**Step 2 — Incomplete dates:**
Compare source row count vs raw row count per date.
Re-process only dates where source_rows > raw_rows.
Captures late-arriving GA4 data without blind re-processing.

**Result:**
- Normal daily run: processes 1 date (yesterday)
- After pipeline failure: automatically catches up on missed dates
- Late-arriving data: only re-processes affected dates, not a fixed N-day window

**Idempotency:** DELETE date + INSERT refreshed date ensures re-running any date produces identical results. The DELETE always runs before INSERT — if INSERT fails, the next run will detect the date as incomplete (source_rows > raw_rows = 0) and re-process it.

---

## Failure Handling

**Extract failures:**
- Retry 3 times with 5-minute delay
- If all retries fail: alert on-call, date remains in `missing_dates` for next run
- pipeline_runs records `status='failed'` with error message for debugging

**dbt failures:**
- dbt test failures halt the pipeline before Sheets export
- Prevents corrupt or incomplete data reaching stakeholders
- Cloud Monitoring alert with failing test names

**Sheets export failures:**
- Retry 3 times
- If all retries fail: BigQuery mart tables are still updated correctly
- Sheets export can be re-triggered manually without re-running extract/transform

**Partial date failures:**
- extract.py processes each date independently
- One failing date does not stop others from processing
- Failed dates automatically retried on next run via missing_dates detection

---

## Retry Strategy

| Layer | Retries | Delay | Strategy |
|---|---|---|---|
| Extract (per date) | 3 | 5 min | Exponential backoff |
| dbt run | 2 | 2 min | Fixed delay |
| dbt test | 0 | — | Fail immediately — data issue requires investigation |
| Sheets export | 3 | 1 min | Fixed delay |

---

## Logging and Monitoring

**pipeline_runs table (BigQuery):**
Every extract run logs: date, status, rows_processed, duration, error_message.
Source of truth for pipeline health. Queryable for operational dashboards.

**Cloud Logging:**
All Python logs streamed to Cloud Logging via structured logging.
Log levels: INFO for normal operations, ERROR for failures.

**Cloud Monitoring alerts:**
- Cloud Run Job failure → PagerDuty/Slack within 5 minutes
- dbt test failure → Email to data team
- Sheets export failure → Slack notification
- Row count anomaly (>20% variance from 7-day avg) → Slack warning

**Sample monitoring query:**
```sql
SELECT
    run_date,
    status,
    rows_processed,
    duration_seconds,
    error_message
FROM pipeline_runs
WHERE started_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
ORDER BY started_at DESC
```

---

## Google Sheets Update Strategy

**Pattern:** Clear → write (full overwrite on each run)

- Dates never duplicate — clearing first guarantees clean state
- Corrected historical data automatically reflected on next run
- Sheet always mirrors mart exactly — no reconciliation logic needed

**Frequency:** Daily after mart refresh. Stakeholders always see previous day's complete data by start of business.

---

## Partitioning Note

Raw tables (`raw_events`, `raw_purchase_items`) are currently non-partitioned due to a GCP project-level restriction encountered during development where DML operations on partitioned tables silently discarded rows into the `__NULL__` partition.

**Production recommendation:**
In a standard GCP project without this restriction, tables should be:
- Partitioned by `event_date_dt` (DATE) for cost-efficient querying
- Clustered by `event_name, user_pseudo_id` for raw_events
- Clustered by `item_name, transaction_id` for raw_purchase_items

This would reduce BigQuery scan costs by 70-90% for date-range queries and improve dbt incremental model performance significantly.

---

## Intermediate Layer Note

The intermediate dbt layer was intentionally omitted for this pipeline. Direct staging → mart is appropriate here because:
- Only 3 mart models exist, each with simple aggregation logic
- The purchases + users join is the only repeated logic across daily and weekly marts
- Adding an intermediate layer would add 2 more BigQuery tables and dbt run steps without meaningful benefit at this scale

In production, an intermediate layer would be introduced if:
- Join logic was reused across 3+ models
- Complex business rules needed centralized definition
- Mart query complexity exceeded maintainable thresholds

---

## Data Quality

**dbt tests run on every pipeline execution:**
- Structural: unique + not_null on all primary keys; FK integrity on stg_purchase_items → stg_purchases
- Business-logic reconciliation: net revenue identity, weekly→daily additive reconciliation, new customers ≤ orders sanity check, conversion rate non-negative

**Source validation:**
All 92 dates validated against source — row counts, revenue, orders, customers, and sessions match the GA4 public dataset exactly.

**Known data characteristics:**
- **Order grain:** `transaction_id` is not unique per order in this obfuscated dataset; orders are counted at the purchase event grain (`event_id`).
- **Weekly customer metrics are non-additive:** `unique_customers` and `returning_customers` are recomputed at week grain from source, not summed from daily.
- **2020-11-01:** 0 purchase metrics — all purchase events that day have NULL `transaction_id` (obfuscation artifact), excluded from orders/revenue.
- first_seen_date is NULL for ~30 purchase-only users with no session_start events (valid, not a data quality issue)