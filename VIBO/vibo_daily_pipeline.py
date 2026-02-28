"""
VIBO Daily Pipeline
==================
Automated daily pipeline that runs in the correct order:

1. sp_Customer360_ETL - Load/refresh data
2. llm_summariser_v4.py - Generate LLM summaries
3. vibo_embedding_pipeline.py - Embed LLM summaries

Usage:
    python VIBO\vibo_daily_pipeline.py                  # Run for today
    python VIBO\vibo_daily_pipeline.py --date 2026-02-17  # Run for specific date
    python VIBO\vibo_daily_pipeline.py --llm-only      # Only run LLM summarizer
    python VIBO\vibo_daily_pipeline.py --embed-only     # Only run embeddings
"""
import argparse
import subprocess
import sys
from datetime import datetime, timedelta
import pyodbc

# Database connection
DB_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=DBUATL01;"
    "DATABASE=Customer_FeedBack_JIT;"
    "Trusted_Connection=yes;"
)


def run_etl(run_date: str) -> bool:
    """Execute sp_Customer360_ETL stored procedure."""
    print(f"\n{'='*60}")
    print(f"STEP 1: RUNNING ETL STORED PROCEDURE")
    print(f"{'='*60}")
    print(f"Run Date: {run_date}")

    conn = pyodbc.connect(DB_CONN_STR)
    cursor = conn.cursor()

    try:
        # Execute the stored procedure
        cursor.execute("DECLARE @return_value int")
        cursor.execute(f"EXEC @return_value = [dbo].[sp_Customer360_ETL] @RunDate = '{run_date}'")
        cursor.execute("SELECT @return_value as return_value")

        result = cursor.fetchone()
        return_value = result[0] if result else None

        conn.close()

        if return_value == 0:
            print(f"✅ ETL completed successfully")
            return True
        else:
            print(f"❌ ETL failed with return code: {return_value}")
            return False

    except Exception as e:
        print(f"❌ ETL failed with error: {e}")
        return False


def run_llm_summarizer(run_date: str) -> bool:
    """Run LLM summarizer for the specified date."""
    print(f"\n{'='*60}")
    print(f"STEP 2: RUNNING LLM SUMMARIZER")
    print(f"{'='*60}")
    print(f"Run Date: {run_date}")

    cmd = ["python", "llm_summariser_v4.py", "--run-date", run_date]

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)  # 2 hour timeout

        # Check for completion
        if "PROCESSING COMPLETE" in result.stderr:
            # Parse results
            for line in result.stderr.split('\n'):
                if "Total Processed" in line or "Inserted" in line or "Updated" in line:
                    print(f"  {line.strip()}")
            print(f"✅ LLM Summarizer completed")
            return True
        else:
            print(f"❌ LLM Summarizer may have failed. Check logs.")
            return False

    except subprocess.TimeoutExpired:
        print(f"❌ LLM Summarizer timed out after 2 hours")
        return False
    except Exception as e:
        print(f"❌ LLM Summarizer failed: {e}")
        return False


def run_embedding_pipeline(full_rebuild: bool = False) -> bool:
    """Run embedding pipeline."""
    print(f"\n{'='*60}")
    print(f"STEP 3: RUNNING EMBEDDING PIPELINE")
    print(f"{'='*60}")

    if full_rebuild:
        print("Mode: FULL REBUILD (re-embed everything)")
    else:
        print("Mode: INCREMENTAL (only new/changed data)")

    cmd = ["python", "VIBO/vibo_embedding_pipeline.py"]
    if full_rebuild:
        cmd.append("--full-rebuild")
    cmd.append("--source")
    cmd.append("summaries")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)  # 1 hour timeout

        # Check for completion
        if "PIPELINE COMPLETE" in result.stderr:
            # Parse results
            for line in result.stderr.split('\n'):
                if "complete:" in line.lower() or "processed:" in line.lower():
                    print(f"  {line.strip()}")
            print(f"✅ Embedding Pipeline completed")
            return True
        else:
            print(f"❌ Embedding Pipeline may have failed. Check logs.")
            return False

    except subprocess.TimeoutExpired:
        print(f"❌ Embedding Pipeline timed out after 1 hour")
        return False
    except Exception as e:
        print(f"❌ Embedding Pipeline failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="VIBO Daily Pipeline - Automated end-to-end execution",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog="""
Examples:
  python VIBO\vibo_daily_pipeline.py                       # Run for today
  python VIBO\vibo_daily_pipeline.py --date 2026-02-17       # Run for Feb 17
  python VIBO\vibo_daily_pipeline.py --full-rebuild          # Full rebuild of embeddings
  python VIBO\vibo_daily_pipeline.py --llm-only              # Only LLM summarizer
  python VIBO\vibo_daily_pipeline.py --embed-only             # Only embeddings
        """,
    )

    parser.add_argument("--date", type=str, default=None,
                       help="Run date (YYYY-MM-DD format). Default: today")
    parser.add_argument("--full-rebuild", action="store_true",
                       help="Full rebuild of embeddings (ignore watermark)")
    parser.add_argument("--llm-only", action="store_true",
                       help="Only run LLM summarizer, skip embeddings")
    parser.add_argument("--embed-only", action="store_true",
                       help="Only run embeddings, skip ETL and LLM")
    parser.add_argument("--skip-etl", action="store_true",
                       help="Skip ETL stored procedure")

    args = parser.parse_args()

    # Determine run date
    if args.date:
        run_date = args.date
    else:
        run_date = datetime.now().strftime("%Y-%m-%d")

    print("="*60)
    print("VIBO DAILY PIPELINE")
    print("="*60)
    print(f"Run Date: {run_date}")
    print(f"Mode: {'FULL REBUILD' if args.full_rebuild else 'INCREMENTAL'}")
    print("="*60)

    success = True

    # Step 1: ETL (skip if --embed-only or --skip-etl)
    if not args.embed_only and not args.skip_etl:
        if not run_etl(run_date):
            print("\n❌ Pipeline stopped: ETL failed")
            success = False

    # Step 2: LLM Summarizer (skip if --embed-only)
    if success and not args.embed_only:
        if not run_llm_summarizer(run_date):
            print("\n⚠️  Pipeline continued despite LLM issues (user can review)")

    # Step 3: Embedding Pipeline (skip if --llm-only)
    if success and not args.llm_only:
        if not run_embedding_pipeline(full_rebuild=args.full_rebuild):
            print("\n❌ Pipeline stopped: Embeddings failed")
            success = False

    # Final status
    print("\n" + "="*60)
    if success:
        print("✅ DAILY PIPELINE COMPLETED SUCCESSFULLY")
    else:
        print("❌ DAILY PIPELINE FAILED - CHECK LOGS")
    print("="*60)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
