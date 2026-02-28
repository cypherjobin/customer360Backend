"""
Customer 360 - Daily Pipeline Orchestrator
===========================================
Runs the complete Customer 360 data pipeline for a specific date.

Pipeline Order:
1. ETL: Load events into Customer360_Events (sliding 30-day window)
2. Transcripts: Load JSON transcript files
3. Revenue Cache: Refresh from IEROXAPP2 (30-day window)
4. Device Assets: Refresh device data from IEROXAPP2 (30-day window)
5. LLM Summarizer: Generate customer summaries
6. VIBO Embeddings: Embed LLM summaries into ChromaDB (AFTER LLM completes)

Prerequisites:
    pip install pyodbc

Usage:
    python run_daily_pipeline.py                    # Run for yesterday
    python run_daily_pipeline.py --date 2026-02-16  # Run for specific date
    python run_daily_pipeline.py --skip-llm         # Skip LLM and VIBO steps
    python run_daily_pipeline.py --skip-vibo        # Skip VIBO embeddings
    python run_daily_pipeline.py --full-rebuild     # Full rebuild of VIBO embeddings
"""

import subprocess
import argparse
import logging
from datetime import datetime, timedelta
import sys
import os

# Use the same Python interpreter that's running this script
PYTHON_EXE = sys.executable

# ============================================================
# CONFIGURATION
# ============================================================

DB_CONFIG = {
    "server": "DBUATL01",
    "database": "Customer_FeedBack_JIT",
}

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(SCRIPT_DIR, "daily_pipeline.log")

# Scripts to run
SCRIPT_TRANSCRIPTS = os.path.join(SCRIPT_DIR, "load_transcripts_v2.py")
SCRIPT_REVENUE = os.path.join(SCRIPT_DIR, "refresh_revenue_cache_simple.py")
SCRIPT_DEVICES = os.path.join(SCRIPT_DIR, "refresh_device_assets.py")
SCRIPT_LLM = os.path.join(SCRIPT_DIR, "llm_summariser_v4.py")
SCRIPT_VIBO_EMBEDDINGS = os.path.join(SCRIPT_DIR, "VIBO", "vibo_embedding_pipeline.py")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================
# STEP 1: ETL - Load Events
# ============================================================

def run_etl(run_date):
    """
    Execute sp_Customer360_ETL stored procedure.
    Loads interactions, cases, and creates pending transcript records.
    """
    logger.info("=" * 60)
    logger.info("STEP 1: ETL - Load Events")
    logger.info("=" * 60)

    sqlcmd = f"""
    sqlcmd -S {DB_CONFIG['server']} -d {DB_CONFIG['database']} -Q "
    EXEC [dbo].[sp_Customer360_ETL] @RunDate = '{run_date}';
    "
    """

    try:
        result = subprocess.run(
            sqlcmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes
        )

        if result.returncode == 0:
            logger.info(f"ETL completed successfully for {run_date}")
            # Log output summary
            for line in result.stdout.split('\n'):
                if 'COMPLETE' in line or 'INSERTED' in line or 'UPDATED' in line:
                    logger.info(line)
            return True
        else:
            logger.error(f"ETL failed with return code {result.returncode}")
            logger.error(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("ETL timed out after 10 minutes")
        return False
    except Exception as e:
        logger.error(f"ETL error: {e}")
        return False


# ============================================================
# STEP 2: Load Transcripts
# ============================================================

def run_transcripts():
    """
    Execute load_transcripts_v2.py to load JSON transcript files.
    """
    logger.info("=" * 60)
    logger.info("STEP 2: Load Transcripts")
    logger.info("=" * 60)

    if not os.path.exists(SCRIPT_TRANSCRIPTS):
        logger.error(f"Script not found: {SCRIPT_TRANSCRIPTS}")
        return False

    try:
        result = subprocess.run(
            [PYTHON_EXE, SCRIPT_TRANSCRIPTS],
            capture_output=True,
            text=True,
            timeout=1800  # 30 minutes - many transcripts
        )

        if result.returncode == 0:
            logger.info("Transcript loading completed")
            # Extract summary from output
            for line in result.stdout.split('\n'):
                if 'COMPLETE' in line or 'Loaded' in line or 'NotFound' in line:
                    logger.info(line)
            return True
        else:
            logger.error(f"Transcript loading failed")
            logger.error(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("Transcript loading timed out after 30 minutes")
        return False
    except Exception as e:
        logger.error(f"Transcript loading error: {e}")
        return False


# ============================================================
# STEP 3: Refresh Revenue Cache
# ============================================================

def run_revenue_cache():
    """
    Execute refresh_revenue_cache_simple.py to update revenue data.
    """
    logger.info("=" * 60)
    logger.info("STEP 3: Refresh Revenue Cache")
    logger.info("=" * 60)

    if not os.path.exists(SCRIPT_REVENUE):
        logger.error(f"Script not found: {SCRIPT_REVENUE}")
        return False

    try:
        result = subprocess.run(
            [PYTHON_EXE, SCRIPT_REVENUE],
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes
        )

        if result.returncode == 0:
            logger.info("Revenue cache refresh completed")
            for line in result.stdout.split('\n'):
                if 'customers' in line.lower() or 'tenure' in line.lower():
                    logger.info(line)
            return True
        else:
            logger.error(f"Revenue cache refresh failed")
            logger.error(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("Revenue cache refresh timed out after 10 minutes")
        return False
    except Exception as e:
        logger.error(f"Revenue cache refresh error: {e}")
        return False


# ============================================================
# STEP 4: Refresh Device Assets
# ============================================================

def run_device_assets():
    """
    Execute refresh_device_assets.py to update device data.
    """
    logger.info("=" * 60)
    logger.info("STEP 4: Refresh Device Assets")
    logger.info("=" * 60)

    if not os.path.exists(SCRIPT_DEVICES):
        logger.error(f"Script not found: {SCRIPT_DEVICES}")
        return False

    try:
        result = subprocess.run(
            [PYTHON_EXE, SCRIPT_DEVICES, "--window-days", "30"],
            capture_output=True,
            text=True,
            timeout=600  # 10 minutes
        )

        if result.returncode == 0:
            logger.info("Device assets refresh completed")
            for line in result.stdout.split('\n'):
                if 'device' in line.lower() or 'customers' in line.lower() or 'records' in line.lower():
                    logger.info(line)
            return True
        else:
            logger.error(f"Device assets refresh failed")
            logger.error(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("Device assets refresh timed out after 10 minutes")
        return False
    except Exception as e:
        logger.error(f"Device assets refresh error: {e}")
        return False


# ============================================================
# STEP 5: LLM Summarizer
# ============================================================

def run_llm_summarizer(run_date):
    """
    Execute llm_summariser_v4.py to generate customer summaries.
    """
    logger.info("=" * 60)
    logger.info("STEP 4: LLM Summarizer")
    logger.info("=" * 60)

    if not os.path.exists(SCRIPT_LLM):
        logger.error(f"Script not found: {SCRIPT_LLM}")
        return False

    try:
        result = subprocess.run(
            [PYTHON_EXE, SCRIPT_LLM, "--run-date", run_date],
            capture_output=True,
            text=True,
            timeout=3600  # 60 minutes - LLM can be slow
        )

        if result.returncode == 0:
            logger.info("LLM summarizer completed")
            for line in result.stdout.split('\n'):
                if 'summaries' in line.lower() or 'customers' in line.lower():
                    logger.info(line)
            return True
        else:
            logger.error(f"LLM summarizer failed")
            logger.error(f"STDOUT: {result.stdout}")
            logger.error(f"STDERR: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("LLM summarizer timed out after 60 minutes")
        return False
    except Exception as e:
        logger.error(f"LLM summarizer error: {e}")
        return False


# ============================================================
# STEP 6: VIBO Embeddings (runs AFTER LLM summarizer)
# ============================================================

def run_vibo_embeddings(full_rebuild=False):
    """
    Execute vibo_embedding_pipeline.py to embed LLM summaries into ChromaDB.

    IMPORTANT: This MUST run AFTER LLM summarizer to capture the latest
    LLM-generated intelligence (sentiment, risk, resolution, etc.)

    Uses LLM_Customer_Summary.rolling_summary_text as the embedding source,
    NOT the basic CallTranscript.call_summary.
    """
    logger.info("=" * 60)
    logger.info("STEP 5: VIBO Embedding Pipeline")
    logger.info("=" * 60)

    if not os.path.exists(SCRIPT_VIBO_EMBEDDINGS):
        logger.error(f"Script not found: {SCRIPT_VIBO_EMBEDDINGS}")
        return False

    try:
        cmd = [PYTHON_EXE, SCRIPT_VIBO_EMBEDDINGS, "--source", "summaries"]
        if full_rebuild:
            cmd.append("--full-rebuild")
            logger.info("Mode: FULL REBUILD (re-embed all LLM summaries)")
        else:
            logger.info("Mode: INCREMENTAL (only new/changed LLM summaries)")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600  # 60 minutes - embeddings can be slow
        )

        # Check for completion marker in stderr
        if "PIPELINE COMPLETE" in result.stderr:
            logger.info("VIBO embedding pipeline completed")
            # Extract summary from stderr
            for line in result.stderr.split('\n'):
                if 'complete:' in line.lower() or 'processed:' in line.lower() or 'embedded:' in line.lower():
                    logger.info(line)
            return True
        else:
            logger.error("VIBO embedding pipeline did not complete successfully")
            logger.error(f"STDERR: {result.stderr}")
            return False

    except subprocess.TimeoutExpired:
        logger.error("VIBO embedding pipeline timed out after 60 minutes")
        return False
    except Exception as e:
        logger.error(f"VIBO embedding pipeline error: {e}")
        return False


# ============================================================
# MAIN ORCHESTRATOR
# ============================================================

def run_pipeline(run_date, skip_llm=False, skip_vibo=False, full_rebuild=False, dry_run=False):
    """
    Run the complete Customer 360 pipeline for a specific date.
    """
    logger.info("=" * 60)
    logger.info("Customer 360 Daily Pipeline")
    logger.info("=" * 60)
    logger.info(f"Run Date     : {run_date}")
    logger.info(f"Skip LLM     : {skip_llm}")
    logger.info(f"Skip VIBO    : {skip_vibo}")
    logger.info(f"Full Rebuild : {full_rebuild}")
    logger.info(f"Dry Run      : {dry_run}")
    logger.info(f"Started At   : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    results = {}

    if dry_run:
        logger.info("DRY RUN - No actual execution")
        logger.info("Would execute:")
        logger.info(f"  1. sp_Customer360_ETL @RunDate = '{run_date}'")
        logger.info(f"  2. python {SCRIPT_TRANSCRIPTS}")
        logger.info(f"  3. python {SCRIPT_REVENUE}")
        logger.info(f"  4. python {SCRIPT_DEVICES} --window-days 30")
        if not skip_llm:
            logger.info(f"  5. python {SCRIPT_LLM} --run-date {run_date}")
            if not skip_vibo:
                logger.info(f"  6. python {SCRIPT_VIBO_EMBEDDINGS} --source summaries" +
                          (" --full-rebuild" if full_rebuild else ""))
        return True

    # Step 1: ETL
    results['etl'] = run_etl(run_date)
    if not results['etl']:
        logger.error("Pipeline stopped: ETL failed")
        return False

    # Step 2: Transcripts
    results['transcripts'] = run_transcripts()
    if not results['transcripts']:
        logger.warning("Transcript loading failed, continuing...")

    # Step 3: Revenue Cache
    results['revenue'] = run_revenue_cache()
    if not results['revenue']:
        logger.warning("Revenue cache refresh failed, continuing...")

    # Step 4: Device Assets
    results['devices'] = run_device_assets()
    if not results['devices']:
        logger.warning("Device assets refresh failed, continuing...")

    # Step 5: LLM Summarizer (optional)
    if not skip_llm:
        results['llm'] = run_llm_summarizer(run_date)
        if not results['llm']:
            logger.warning("LLM summarizer failed, but pipeline completed")
    else:
        results['llm'] = None
        logger.info("LLM summarizer skipped (--skip-llm flag)")

    # Step 6: VIBO Embeddings (runs after LLM, skip if --skip-llm or --skip-vibo)
    if not skip_llm and not skip_vibo:
        results['vibo'] = run_vibo_embeddings(full_rebuild=full_rebuild)
        if not results['vibo']:
            logger.warning("VIBO embedding pipeline failed, but pipeline completed")
    else:
        results['vibo'] = None
        if skip_llm:
            logger.info("VIBO embeddings skipped (LLM was skipped)")
        elif skip_vibo:
            logger.info("VIBO embeddings skipped (--skip-vibo flag)")

    # Summary
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info(f"ETL        : {'SUCCESS' if results.get('etl') else 'FAILED'}")
    logger.info(f"Transcripts: {'SUCCESS' if results.get('transcripts') else 'FAILED'}")
    logger.info(f"Revenue    : {'SUCCESS' if results.get('revenue') else 'FAILED'}")
    logger.info(f"Devices    : {'SUCCESS' if results.get('devices') else 'FAILED'}")
    if results.get('llm') is not None:
        logger.info(f"LLM        : {'SUCCESS' if results.get('llm') else 'FAILED'}")
    if results.get('vibo') is not None:
        logger.info(f"VIBO       : {'SUCCESS' if results.get('vibo') else 'FAILED'}")
    logger.info(f"Completed At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    return all(v for v in results.values() if v is not None)


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Customer 360 Daily Pipeline Orchestrator",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Examples:
  python run_daily_pipeline.py                     # Run for yesterday
  python run_daily_pipeline.py --date 2026-02-16   # Run for Feb 16
  python run_daily_pipeline.py --skip-llm          # Skip LLM and VIBO
  python run_daily_pipeline.py --skip-vibo         # Skip VIBO embeddings only
  python run_daily_pipeline.py --full-rebuild      # Full rebuild of VIBO embeddings
        """
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Run date (YYYY-MM-DD). Default: yesterday"
    )
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        help="Skip LLM summarizer and VIBO embeddings steps"
    )
    parser.add_argument(
        "--skip-vibo",
        action="store_true",
        help="Skip VIBO embeddings step (runs LLM but not embeddings)"
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Full rebuild of VIBO embeddings (ignore watermark)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be executed without running"
    )

    args = parser.parse_args()

    # Determine run date
    if args.date:
        try:
            run_date = datetime.strptime(args.date, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        run_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        logger.info(f"No date specified, using yesterday: {run_date}")

    # Run pipeline
    success = run_pipeline(
        run_date=run_date,
        skip_llm=args.skip_llm,
        skip_vibo=args.skip_vibo,
        full_rebuild=args.full_rebuild,
        dry_run=args.dry_run
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
