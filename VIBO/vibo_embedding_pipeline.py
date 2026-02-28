"""
VIBO Embedding Pipeline
========================
Daily ETL that extracts LLM summaries from LLM_Customer_Summary and events from Customer360_Events,
generates embeddings, and upserts them into ChromaDB.

IMPORTANT: Must run AFTER llm_summariser_v4.py to capture the latest LLM-generated intelligence.

Changes (v3.0 - LLM Summary Embedding):
- NOW: Embeds LLM_Customer_Summary.rolling_summary_text (rich with sentiment, risk, etc.)
- PREVIOUSLY: Embedded CallTranscript.call_summary (basic summary only)

Usage:
    python vibo_embedding_pipeline.py                    # Incremental (delta only)
    python vibo_embedding_pipeline.py --full-rebuild      # Full rebuild of all embeddings
    python vibo_embedding_pipeline.py --source transcripts  # Only transcripts
    python vibo_embedding_pipeline.py --source events       # Only events
    python vibo_embedding_pipeline.py --customer 10900099   # Single customer
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, date
from typing import Optional

from vibo_config import (
    EMBED_BATCH_SIZE, EMBED_MAX_TEXT_LENGTH,
    CHROMA_COLLECTION_TRANSCRIPTS, CHROMA_COLLECTION_EVENTS,
    EMBEDDING_PROVIDER, OLLAMA_EMBED_MODEL, AZURE_EMBED_DEPLOYMENT,
    LOG_LEVEL,
)
from vibo_database import db_cursor
from vibo_vector_store import VectorStore

# ─── Logging ───
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("vibo.embed_pipeline")


# ═══════════════════════════════════════════════════════════════════════════════
# WATERMARK MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════
def get_last_watermark(source_table: str, collection_name: str) -> Optional[datetime]:
    """Get the last processed watermark timestamp for a source table."""
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT TOP 1 watermark_timestamp
            FROM VIBO_Embedding_Log
            WHERE source_table = ?
              AND collection_name = ?
              AND status = 'COMPLETED'
            ORDER BY run_date DESC, log_id DESC
        """, source_table, collection_name)
        row = cursor.fetchone()
        return row.watermark_timestamp if row else None


def log_embedding_run(
    source_table: str,
    collection_name: str,
    records_processed: int,
    records_skipped: int,
    records_failed: int,
    duration_seconds: int,
    watermark_timestamp: Optional[datetime],
    status: str = "COMPLETED",
    error_message: str = None,
):
    """Log an embedding pipeline run."""
    model = OLLAMA_EMBED_MODEL if EMBEDDING_PROVIDER == "ollama" else AZURE_EMBED_DEPLOYMENT
    
    with db_cursor(commit=True) as cursor:
        cursor.execute("""
            INSERT INTO VIBO_Embedding_Log 
                (run_date, source_table, collection_name, records_processed,
                 records_skipped, records_failed, embedding_model, 
                 duration_seconds, status, error_message, watermark_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            date.today(), source_table, collection_name, records_processed,
            records_skipped, records_failed, model,
            duration_seconds, status, error_message, watermark_timestamp,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# TEXT EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════
def extract_transcripts(
    watermark: Optional[datetime] = None,
    customer_id: Optional[str] = None,
) -> list[dict]:
    """
    Extract LLM customer summaries for embedding.

    NOW USES: LLM_Customer_Summary table (rich intelligence from LLM)
    PREVIOUSLY: CallTranscript table (basic summaries)

    This change ensures we embed:
    - Sentiment analysis
    - Escalation risk scores
    - Resolution status
    - Customer health indicators
    - Churn risk
    - Contact timeline
    - Key issues

    Returns list of: {
        "id": customer_id,
        "text": rolling_summary_text (full LLM-generated summary),
        "metadata": { customer_id, last_full_build_date, escalation_risk, sentiment, resolution, source }
    }
    """
    conditions = ["rolling_summary_text IS NOT NULL", "LEN(rolling_summary_text) > 100"]
    params = []

    if watermark:
        # Check both last_full_build_date (for full rebuilds) AND updated_date (for incremental updates)
        conditions.append("(last_full_build_date > ? OR updated_date > ?)")
        params.append(watermark)
        params.append(watermark)

    if customer_id:
        conditions.append("customer_id = ?")
        params.append(customer_id)

    where_clause = " AND ".join(conditions)

    with db_cursor() as cursor:
        cursor.execute(f"""
            SELECT
                customer_id,
                rolling_summary_text,
                last_full_build_date,
                last_processed_event_ts,
                escalation_risk_score,
                summary_json
            FROM LLM_Customer_Summary
            WHERE {where_clause}
            ORDER BY last_full_build_date ASC
        """, *params)

        documents = []
        for row in cursor.fetchall():
            # Use the full LLM-generated rolling summary
            full_text = row.rolling_summary_text

            # Truncate if needed (LLM summaries can be long)
            if len(full_text) > EMBED_MAX_TEXT_LENGTH:
                full_text = full_text[:EMBED_MAX_TEXT_LENGTH]

            documents.append({
                "id": str(row.customer_id),
                "text": full_text,
                "metadata": {
                    "customer_id": str(row.customer_id),
                    "last_full_build_date": row.last_full_build_date.isoformat() if row.last_full_build_date else "",
                    "last_processed_event_ts": row.last_processed_event_ts.isoformat() if row.last_processed_event_ts else "",
                    "escalation_risk_score": str(row.escalation_risk_score) if row.escalation_risk_score is not None else "",
                    "source": "LLM_Customer_Summary",
                },
            })

        logger.info(f"Extracted {len(documents)} LLM summary documents")
        return documents


def extract_events(
    watermark: Optional[datetime] = None,
    customer_id: Optional[str] = None,
) -> list[dict]:
    """
    Extract customer event text for embedding.
    
    Extracts readable text from event_detail_json for each event.
    Only includes events with meaningful text content.
    
    Returns list of: {
        "id": natural_key,
        "text": extracted event text,
        "metadata": { customer_id, event_type, source_system, event_timestamp, source }
    }
    """
    conditions = [
        "event_detail_json IS NOT NULL",
        "LEN(event_detail_json) > 20",
        "(is_deleted = 0 OR is_deleted IS NULL)",
    ]
    params = []
    
    if watermark:
        conditions.append("event_timestamp > ?")
        params.append(watermark)
    
    if customer_id:
        conditions.append("customer_id = ?")
        params.append(customer_id)
    
    where_clause = " AND ".join(conditions)
    
    with db_cursor() as cursor:
        cursor.execute(f"""
            SELECT 
                natural_key,
                customer_id,
                event_type,
                source_system,
                event_timestamp,
                event_detail_json
            FROM Customer360_Events
            WHERE {where_clause}
            ORDER BY event_timestamp ASC
        """, *params)
        
        documents = []
        skipped = 0
        
        for row in cursor.fetchall():
            # Extract readable text from the JSON
            text = _extract_event_text(row.event_detail_json, row.event_type, row.source_system)
            
            if not text or len(text) < 20:
                skipped += 1
                continue
            
            # Truncate if needed
            if len(text) > EMBED_MAX_TEXT_LENGTH:
                text = text[:EMBED_MAX_TEXT_LENGTH]
            
            documents.append({
                "id": str(row.natural_key),
                "text": text,
                "metadata": {
                    "customer_id": str(row.customer_id),
                    "event_type": row.event_type or "",
                    "source_system": row.source_system or "",
                    "event_timestamp": row.event_timestamp.isoformat() if row.event_timestamp else "",
                    "source": "Customer360_Events",
                },
            })
        
        logger.info(f"Extracted {len(documents)} event documents (skipped {skipped} with no text)")
        return documents


def _extract_event_text(json_str: str, event_type: str, source_system: str) -> str:
    """
    Extract readable text from event_detail_json.
    Handles different JSON structures across Interaction, Pega, ServiceNow, CallRecording.
    """
    try:
        detail = json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return ""
    
    if not isinstance(detail, dict):
        return str(detail)[:EMBED_MAX_TEXT_LENGTH]
    
    text_parts = []
    
    # Add context prefix
    if event_type:
        text_parts.append(f"Event type: {event_type}.")
    if source_system:
        text_parts.append(f"Source: {source_system}.")
    
    # Extract text from known fields (order matters - most informative first)
    text_fields = [
        "description", "short_description", "summary", "call_summary",
        "notes", "wrap_up_comment", "agent_notes", "resolution_notes",
        "close_notes", "work_notes", "additional_comments",
        "case_description", "incident_description",
        "type", "sub_type", "case_type", "category", "subcategory",
        "status", "priority", "urgency", "impact",
        "assigned_to", "assignment_group",
        "resolution", "resolution_code",
    ]
    
    for field in text_fields:
        value = detail.get(field)
        if value and isinstance(value, str) and len(value.strip()) > 2:
            # Avoid duplicating short labels we already have in context
            if field in ("type", "sub_type", "case_type", "category", "subcategory", 
                         "status", "priority", "assigned_to"):
                text_parts.append(f"{field.replace('_', ' ').title()}: {value.strip()}.")
            else:
                text_parts.append(value.strip())
    
    return " ".join(text_parts)


# ═══════════════════════════════════════════════════════════════════════════════
# PIPELINE EXECUTION
# ═══════════════════════════════════════════════════════════════════════════════
def run_transcript_pipeline(
    store: VectorStore,
    full_rebuild: bool = False,
    customer_id: Optional[str] = None,
) -> dict:
    """Run the LLM summary embedding pipeline."""
    source_table = "LLM_Customer_Summary"
    collection_name = CHROMA_COLLECTION_TRANSCRIPTS

    logger.info(f"{'='*60}")
    logger.info(f"LLM SUMMARY PIPELINE: {'FULL REBUILD' if full_rebuild else 'INCREMENTAL'}")
    logger.info(f"{'='*60}")

    start_time = time.time()

    # Get watermark
    watermark = None if full_rebuild else get_last_watermark(source_table, collection_name)
    if watermark:
        logger.info(f"Watermark: {watermark.isoformat()}")
    else:
        logger.info("No watermark - processing all records")

    # Full rebuild: reset collection
    if full_rebuild and not customer_id:
        logger.info("Resetting LLM summary collection...")
        store.reset_collection(collection_name)
        store.transcripts_collection = store.client.get_or_create_collection(
            name=collection_name, metadata={"hnsw:space": "cosine"}
        )
    
    try:
        # Extract
        documents = extract_transcripts(watermark=watermark, customer_id=customer_id)
        
        if not documents:
            logger.info("No new transcripts to embed.")
            duration = int(time.time() - start_time)
            log_embedding_run(source_table, collection_name, 0, 0, 0, duration, watermark)
            return {"processed": 0, "skipped": 0, "failed": 0, "duration": duration}
        
        # Embed and upsert
        processed = store.upsert_transcripts(documents)

        # Update watermark to the latest last_full_build_date we processed
        new_watermark = max(
            (datetime.fromisoformat(d["metadata"]["last_full_build_date"])
             for d in documents if d["metadata"]["last_full_build_date"]),
            default=watermark,
        )

        duration = int(time.time() - start_time)
        log_embedding_run(source_table, collection_name, processed, 0, 0, duration, new_watermark)

        logger.info(f"LLM summary pipeline complete: {processed} docs in {duration}s")
        return {"processed": processed, "skipped": 0, "failed": 0, "duration": duration}
        
    except Exception as e:
        duration = int(time.time() - start_time)
        log_embedding_run(source_table, collection_name, 0, 0, 0, duration, watermark,
                         status="FAILED", error_message=str(e)[:500])
        logger.error(f"LLM summary pipeline failed: {e}")
        raise


def run_event_pipeline(
    store: VectorStore,
    full_rebuild: bool = False,
    customer_id: Optional[str] = None,
) -> dict:
    """Run the event embedding pipeline."""
    source_table = "Customer360_Events"
    collection_name = CHROMA_COLLECTION_EVENTS
    
    logger.info(f"{'='*60}")
    logger.info(f"EVENT PIPELINE: {'FULL REBUILD' if full_rebuild else 'INCREMENTAL'}")
    logger.info(f"{'='*60}")
    
    start_time = time.time()
    
    # Get watermark
    watermark = None if full_rebuild else get_last_watermark(source_table, collection_name)
    if watermark:
        logger.info(f"Watermark: {watermark.isoformat()}")
    else:
        logger.info("No watermark - processing all records")
    
    # Full rebuild: reset collection
    if full_rebuild and not customer_id:
        logger.info("Resetting events collection...")
        store.reset_collection(collection_name)
        store.events_collection = store.client.get_or_create_collection(
            name=collection_name, metadata={"hnsw:space": "cosine"}
        )
    
    try:
        # Extract
        documents = extract_events(watermark=watermark, customer_id=customer_id)
        
        if not documents:
            logger.info("No new events to embed.")
            duration = int(time.time() - start_time)
            log_embedding_run(source_table, collection_name, 0, 0, 0, duration, watermark)
            return {"processed": 0, "skipped": 0, "failed": 0, "duration": duration}
        
        # Embed and upsert in batches
        processed = store.upsert_events(documents)
        
        # Update watermark
        new_watermark = max(
            (datetime.fromisoformat(d["metadata"]["event_timestamp"]) 
             for d in documents if d["metadata"]["event_timestamp"]),
            default=watermark,
        )
        
        duration = int(time.time() - start_time)
        log_embedding_run(source_table, collection_name, processed, 0, 0, duration, new_watermark)
        
        logger.info(f"Event pipeline complete: {processed} docs in {duration}s")
        return {"processed": processed, "skipped": 0, "failed": 0, "duration": duration}
        
    except Exception as e:
        duration = int(time.time() - start_time)
        log_embedding_run(source_table, collection_name, 0, 0, 0, duration, watermark,
                         status="FAILED", error_message=str(e)[:500])
        logger.error(f"Event pipeline failed: {e}")
        raise


def run_full_pipeline(
    full_rebuild: bool = False,
    source: str = "all",
    customer_id: Optional[str] = None,
) -> dict:
    """
    Run the complete embedding pipeline.

    Args:
        full_rebuild: If True, re-embed everything (ignore watermarks)
        source: "all", "summaries", or "events"
        customer_id: Optional - embed only for a specific customer
    """
    logger.info("=" * 60)
    logger.info("VIBO EMBEDDING PIPELINE")
    logger.info(f"Mode: {'FULL REBUILD' if full_rebuild else 'INCREMENTAL'}")
    logger.info(f"Source: {source}")
    if customer_id:
        logger.info(f"Customer: {customer_id}")
    logger.info("=" * 60)

    start_time = time.time()

    # Initialize vector store
    store = VectorStore()
    store.initialize()

    results = {}

    # Run LLM summary pipeline (previously transcript pipeline)
    if source in ("all", "summaries", "transcripts"):  # Keep "transcripts" for backward compatibility
        try:
            results["summaries"] = run_transcript_pipeline(
                store, full_rebuild=full_rebuild, customer_id=customer_id
            )
        except Exception as e:
            results["summaries"] = {"error": str(e)}
            logger.error(f"LLM summary pipeline failed: {e}")
    
    # Run event pipeline
    if source in ("all", "events"):
        try:
            results["events"] = run_event_pipeline(
                store, full_rebuild=full_rebuild, customer_id=customer_id
            )
        except Exception as e:
            results["events"] = {"error": str(e)}
            logger.error(f"Event pipeline failed: {e}")
    
    total_duration = int(time.time() - start_time)
    
    # Final stats
    stats = store.get_stats()
    results["vector_store_stats"] = stats
    results["total_duration_seconds"] = total_duration
    
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"Total duration: {total_duration}s")
    logger.info(f"Summaries in store: {stats['transcripts_count']}")
    logger.info(f"Events in store: {stats['events_count']}")
    logger.info("=" * 60)
    
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# CLI ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(
        description="VIBO Embedding Pipeline - Embeds LLM summaries and events into ChromaDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python vibo_embedding_pipeline.py                        # Incremental run
  python vibo_embedding_pipeline.py --full-rebuild         # Full rebuild
  python vibo_embedding_pipeline.py --source summaries     # LLM summaries only
  python vibo_embedding_pipeline.py --source events        # Events only
  python vibo_embedding_pipeline.py --customer 10900099    # Single customer
  python vibo_embedding_pipeline.py --stats                # Show stats only
        """,
    )
    parser.add_argument("--full-rebuild", action="store_true",
                       help="Full rebuild (re-embed everything)")
    parser.add_argument("--source", choices=["all", "summaries", "transcripts", "events"],
                       default="all", help="Which source to process")
    parser.add_argument("--customer", type=str, default=None,
                       help="Process only a specific customer ID")
    parser.add_argument("--stats", action="store_true",
                       help="Show vector store stats and exit")
    
    args = parser.parse_args()
    
    if args.stats:
        store = VectorStore()
        store.initialize()
        stats = store.get_stats()
        print(f"\nVector Store Statistics:")
        print(f"  Transcripts: {stats['transcripts_count']} documents")
        print(f"  Events: {stats['events_count']} documents")
        print(f"  Storage: {stats['chroma_path']}")
        print(f"  Provider: {stats['embedding_provider']}")
        
        # Show last run info
        with db_cursor() as cursor:
            cursor.execute("""
                SELECT TOP 5 
                    run_date, source_table, records_processed,
                    duration_seconds, status, watermark_timestamp
                FROM VIBO_Embedding_Log
                ORDER BY log_id DESC
            """)
            rows = cursor.fetchall()
            if rows:
                print(f"\n  Last {len(rows)} runs:")
                for r in rows:
                    print(f"    {r.run_date} | {r.source_table:<20} | "
                          f"{r.records_processed:>6} docs | {r.duration_seconds:>4}s | {r.status}")
        return
    
    results = run_full_pipeline(
        full_rebuild=args.full_rebuild,
        source=args.source,
        customer_id=args.customer,
    )
    
    # Print summary
    print(f"\n{'='*60}")
    print("EMBEDDING PIPELINE RESULTS")
    print(f"{'='*60}")
    for key, val in results.items():
        if isinstance(val, dict):
            print(f"\n{key}:")
            for k2, v2 in val.items():
                print(f"  {k2}: {v2}")
        else:
            print(f"{key}: {val}")


if __name__ == "__main__":
    main()
