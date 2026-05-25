"""
pipeline.py
===========
Orchestrates the full GA4 ecommerce ETL pipeline end to end.

Steps:
1. Extract  — load raw data from GA4 source into BigQuery
2. Transform — run dbt models (staging → mart)
3. Test     — run dbt tests as quality gate
4. Export   — push mart metrics to Google Sheets

Usage:
    python python/pipeline.py                   # full run
    python python/pipeline.py --date 20201101   # specific date only

Design:
- Each step only runs if previous succeeds
- Pipeline fails fast on any step error
- Exit code 0 = success, 1 = failure
"""

import argparse
import logging
import subprocess
import sys
from datetime import datetime, timezone

import yaml

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
# STEP RUNNER
# ─────────────────────────────────────────────
def run_step(
    name: str,
    command: list,
    cwd: str = None
) -> bool:
    """
    Runs a pipeline step as a subprocess.
    Returns True if successful, False if failed.
    Streams output in real time.
    """
    log.info(f"Starting: {name}")
    log.info(f"Command:  {' '.join(command)}")
    started_at = datetime.now(timezone.utc)

    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            check=True,
            text=True
        )
        duration = (datetime.now(timezone.utc) - started_at).total_seconds()
        log.info(f"✅ Completed: {name} ({duration:.1f}s)")
        return True

    except subprocess.CalledProcessError as e:
        duration = (datetime.now(timezone.utc) - started_at).total_seconds()
        log.error(f"❌ Failed: {name} ({duration:.1f}s)")
        log.error(f"Exit code: {e.returncode}")
        return False


# ─────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────
def run_pipeline(
    config_path: str = 'config/config.yaml',
    date_override: str = None
) -> bool:
    """
    Runs the full ETL pipeline.
    Returns True if all steps succeed, False otherwise.
    """
    started_at = datetime.now(timezone.utc)

    log.info("=" * 60)
    log.info("GA4 ECOMMERCE ETL PIPELINE")
    log.info(f"Started: {started_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    if date_override:
        log.info(f"Date override: {date_override}")
    log.info("=" * 60)

    # ── Step 1: Extract ───────────────────────
    extract_cmd = ["python", "python/extract.py", "--config", config_path]
    if date_override:
        extract_cmd += ["--date", date_override]

    if not run_step("Extract — raw layer", extract_cmd):
        log.error("Pipeline failed at Extract step")
        return False

    # ── Step 2: Transform ─────────────────────
    if not run_step(
        "Transform — dbt run",
        ["dbt", "run", "--select", "staging mart"],
        cwd="dbt"
    ):
        log.error("Pipeline failed at Transform step")
        return False

    # ── Step 3: Test ──────────────────────────
    if not run_step(
        "Test — dbt test",
        ["dbt", "test"],
        cwd="dbt"
    ):
        log.error("Pipeline failed at Test step — data quality check failed")
        return False

    # ── Step 4: Export ────────────────────────
    if not run_step(
        "Export — Google Sheets",
        ["python", "python/sheets.py", "--config", config_path]
    ):
        log.error("Pipeline failed at Export step")
        return False

    # ── Summary ───────────────────────────────
    duration = (datetime.now(timezone.utc) - started_at).total_seconds()
    log.info("=" * 60)
    log.info(f"✅ PIPELINE COMPLETE ({duration:.1f}s)")
    log.info("=" * 60)
    return True


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Run the full GA4 ecommerce ETL pipeline'
    )
    parser.add_argument(
        '--date',
        type=str,
        help='Process specific date only (YYYYMMDD). '
             'Passed through to extract.py.',
        required=False
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config/config.yaml',
        help='Path to config file (default: config/config.yaml)'
    )
    args = parser.parse_args()

    success = run_pipeline(
        config_path=args.config,
        date_override=args.date
    )

    sys.exit(0 if success else 1)
