"""
Customer 360 - Call Transcript Loader v2
=========================================
Loads JSON transcript files from the network drive into the
CallTranscript table, extracts pre-analysed intelligence
(summary, issues, quotes), and links events to transcripts.

Target Table: [Customer_FeedBack_JIT].[dbo].[CallTranscript]

Prerequisites:
    pip install pyodbc

Usage:
    python load_transcripts_v2.py           Load all pending transcripts
    python load_transcripts_v2.py verify    Check loading status
    python load_transcripts_v2.py retry     Retry previously NotFound files
    python load_transcripts_v2.py link      Re-link events to transcripts
    python load_transcripts_v2.py help      Show help
"""

import pyodbc
import json
import os
import logging
from datetime import datetime

# ============================================================
# CONFIGURATION
# ============================================================

DB_CONFIG = {
    "server": "DBUATL01",
    "database": "Customer_FeedBack_JIT",
    "driver": "{ODBC Driver 17 for SQL Server}",
    "trusted_connection": "yes",
}

BASE_PATH = r"Y:\AI_DATA\results"
BATCH_SIZE = 50
LOG_FILE = "transcript_loader_v2.log"

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
# DATABASE
# ============================================================

def get_connection():
    conn_str = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
    )
    if DB_CONFIG.get("trusted_connection") == "yes":
        conn_str += "Trusted_Connection=yes;"
    else:
        conn_str += f"UID={DB_CONFIG['uid']};PWD={DB_CONFIG['pwd']};"
    return pyodbc.connect(conn_str)


# ============================================================
# FETCH PENDING
# ============================================================

def get_pending_recordings(conn):
    query = """
        SELECT 
            [transcript_id], 
            [customer_id], 
            [audio_filename], 
            [call_start],
            [call_end],
            [audio_duration],
            [json_date_folder],
            [json_filename],
            [json_full_filepath]
        FROM [dbo].[CallTranscript]
        WHERE [transcript_status] = 'Pending'
        ORDER BY [customer_id], [call_start]
    """
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return rows


# ============================================================
# READ JSON FILE
# ============================================================

def read_transcript_json(filepath):
    if not filepath:
        return None, "Error"

    filepath = os.path.normpath(filepath)

    if not os.path.exists(filepath):
        return None, "NotFound"

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            content = json.load(f)
        return json.dumps(content, ensure_ascii=False), "Loaded"
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {filepath}: {e}")
        return None, "Error"
    except PermissionError:
        logger.error(f"Permission denied: {filepath}")
        return None, "Error"
    except Exception as e:
        logger.error(f"Error reading {filepath}: {e}")
        return None, "Error"


# ============================================================
# EXTRACT INTELLIGENCE FROM TRANSCRIPT
# ============================================================

def extract_intelligence(transcript_json_str):
    """
    Parse transcript JSON and extract pre-analysed fields:
    summary, segment, product, issues, root_causes, quotes.
    """
    if not transcript_json_str:
        return {}

    try:
        data = json.loads(transcript_json_str)
    except (json.JSONDecodeError, TypeError):
        return {}

    extracted = {
        "call_summary": data.get("summary"),
        "call_segment": data.get("segment"),
        "call_product": data.get("product"),
        "call_issues_json": None,
        "call_root_causes_json": None,
        "customer_quotes_json": None,
    }

    if data.get("issues"):
        extracted["call_issues_json"] = json.dumps(data["issues"], ensure_ascii=False)
    if data.get("root_causes"):
        extracted["call_root_causes_json"] = json.dumps(data["root_causes"], ensure_ascii=False)
    if data.get("salient_quotes"):
        extracted["customer_quotes_json"] = json.dumps(data["salient_quotes"], ensure_ascii=False)

    return extracted


# ============================================================
# UPDATE CALLTRANSCRIPT TABLE
# ============================================================

def update_transcript(conn, transcript_id, json_content, status, intelligence=None):
    intel = intelligence or {}
    query = """
        UPDATE [dbo].[CallTranscript]
        SET [transcript_json] = ?,
            [transcript_status] = ?,
            [loaded_at] = GETDATE(),
            [updated_at] = GETDATE(),
            [call_summary] = ?,
            [call_segment] = ?,
            [call_product] = ?,
            [call_issues_json] = ?,
            [call_root_causes_json] = ?,
            [customer_quotes_json] = ?
        WHERE [transcript_id] = ?
    """
    cursor = conn.cursor()
    cursor.execute(query, (
        json_content,
        status,
        intel.get("call_summary"),
        intel.get("call_segment"),
        intel.get("call_product"),
        intel.get("call_issues_json"),
        intel.get("call_root_causes_json"),
        intel.get("customer_quotes_json"),
        transcript_id
    ))
    conn.commit()
    cursor.close()


def transcript_already_loaded(conn, transcript_id):
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT [transcript_status]
            FROM [dbo].[CallTranscript]
            WHERE [transcript_id] = ?
        """, (transcript_id,))
        row = cursor.fetchone()
        return row and str(row[0]).lower() == 'loaded'
    finally:
        cursor.close()


# ============================================================
# LINK EVENTS TO TRANSCRIPTS
# ============================================================

def link_events_to_transcripts(conn):
    """
    Ensure Customer360_Events CallRecording rows have transcript_id set.
    Matches on natural_key = audio_filename|call_start.
    """
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE e
        SET e.[transcript_id] = t.[transcript_id],
            e.[updated_at] = GETDATE()
        FROM [dbo].[Customer360_Events] e
        INNER JOIN [dbo].[CallTranscript] t
            ON e.[natural_key] = CONCAT(t.[audio_filename], '|', CONVERT(VARCHAR(23), t.[call_start], 126))
        WHERE e.[source_system] = 'CallRecording'
          AND e.[transcript_id] IS NULL
    """)
    linked = cursor.rowcount
    conn.commit()
    cursor.close()
    logger.info(f"Linked {linked} events to transcripts")
    return linked


# ============================================================
# UPDATE EVENT HASH WHEN TRANSCRIPT LOADS
# ============================================================

def update_event_hash_for_loaded_transcripts(conn):
    """
    When a transcript changes from Pending to Loaded, update the
    event's change_hash so the summariser knows to re-process.
    """
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE e
        SET e.[change_hash] = HASHBYTES('SHA2_256',
                CONCAT(e.[natural_key], '|', 'Loaded')),
            e.[updated_at] = GETDATE()
        FROM [dbo].[Customer360_Events] e
        INNER JOIN [dbo].[CallTranscript] t ON e.[transcript_id] = t.[transcript_id]
        WHERE e.[source_system] = 'CallRecording'
          AND t.[transcript_status] = 'Loaded'
          AND e.[change_hash] <> HASHBYTES('SHA2_256',
                CONCAT(e.[natural_key], '|', 'Loaded'))
    """)
    updated = cursor.rowcount
    conn.commit()
    cursor.close()
    if updated > 0:
        logger.info(f"Updated change_hash for {updated} events (transcript loaded)")
    return updated


# ============================================================
# MAIN
# ============================================================

def process_transcripts():
    logger.info("=" * 60)
    logger.info("Customer 360 - Transcript Loader v2")
    logger.info("=" * 60)

    try:
        conn = get_connection()
        logger.info(f"Connected to {DB_CONFIG['server']}/{DB_CONFIG['database']}")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return

    pending = get_pending_recordings(conn)
    total = len(pending)
    logger.info(f"Found {total} pending recordings")

    if total == 0:
        logger.info("Nothing to process.")
        conn.close()
        return

    loaded = 0
    not_found = 0
    errors = 0
    skipped = 0
    intel_count = 0

    for i, record in enumerate(pending, 1):
        tid = record["transcript_id"]
        cid = record["customer_id"]
        filepath = record["json_full_filepath"]
        filename = record.get("json_filename", "")

        try:
            if transcript_already_loaded(conn, tid):
                skipped += 1
                continue
        except Exception as e:
            logger.error(f"Check failed for transcript_id {tid}: {e}")

        json_content, status = read_transcript_json(filepath)

        intelligence = {}
        if status == "Loaded" and json_content:
            intelligence = extract_intelligence(json_content)
            if intelligence.get("call_summary"):
                intel_count += 1

        try:
            update_transcript(conn, tid, json_content, status, intelligence)
        except Exception as e:
            logger.error(f"DB update failed for transcript_id {tid}: {e}")
            status = "Error"

        if status == "Loaded":
            loaded += 1
            logger.info(f"[{i}/{total}] LOADED  | {cid} | {filename}"
                        f"{' | Intel: YES' if intelligence.get('call_summary') else ''}")
        elif status == "NotFound":
            not_found += 1
            logger.warning(f"[{i}/{total}] NOT FOUND | {cid} | {filepath}")
        else:
            errors += 1
            logger.error(f"[{i}/{total}] ERROR | {cid} | {filename}")

        if i % BATCH_SIZE == 0:
            logger.info(f"Progress: {i}/{total} "
                        f"(Loaded: {loaded}, NotFound: {not_found}, Errors: {errors})")

    # Post-processing
    logger.info("Post-processing: linking events and updating hashes...")
    link_events_to_transcripts(conn)
    update_event_hash_for_loaded_transcripts(conn)

    logger.info("=" * 60)
    logger.info("COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total Pending      : {total}")
    logger.info(f"Loaded             : {loaded}")
    logger.info(f"Skipped (existing) : {skipped}")
    logger.info(f"Not Found          : {not_found}")
    logger.info(f"Errors             : {errors}")
    logger.info(f"Intel Extracted    : {intel_count}")
    if (loaded + not_found + errors) > 0:
        logger.info(f"Success Rate       : {loaded/(loaded+not_found+errors)*100:.1f}%")
    logger.info("=" * 60)

    conn.close()


# ============================================================
# VERIFY
# ============================================================

def verify_results():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT [transcript_status], COUNT(*) AS cnt
        FROM [dbo].[CallTranscript]
        GROUP BY [transcript_status]
    """)
    print("\n--- Transcript Status ---")
    for row in cursor.fetchall():
        print(f"  {str(row[0]):12s} : {row[1]}")

    cursor.execute("""
        SELECT 
            COUNT(*) AS total_loaded,
            SUM(CASE WHEN [call_summary] IS NOT NULL THEN 1 ELSE 0 END) AS with_summary,
            SUM(CASE WHEN [call_issues_json] IS NOT NULL THEN 1 ELSE 0 END) AS with_issues,
            SUM(CASE WHEN [customer_quotes_json] IS NOT NULL THEN 1 ELSE 0 END) AS with_quotes
        FROM [dbo].[CallTranscript]
        WHERE [transcript_status] = 'Loaded'
    """)
    row = cursor.fetchone()
    if row:
        print(f"\n--- Intelligence Extraction ---")
        print(f"  Loaded transcripts : {row[0]}")
        print(f"  With summary       : {row[1]}")
        print(f"  With issues        : {row[2]}")
        print(f"  With quotes        : {row[3]}")

    cursor.execute("""
        SELECT 
            COUNT(*) AS total,
            SUM(CASE WHEN [transcript_id] IS NOT NULL THEN 1 ELSE 0 END) AS linked,
            SUM(CASE WHEN [transcript_id] IS NULL THEN 1 ELSE 0 END) AS unlinked
        FROM [dbo].[Customer360_Events]
        WHERE [source_system] = 'CallRecording'
    """)
    row = cursor.fetchone()
    if row:
        print(f"\n--- Event-Transcript Linkage ---")
        print(f"  Recording events : {row[0]}")
        print(f"  Linked           : {row[1]}")
        print(f"  Unlinked         : {row[2]}")

    cursor.execute("""
        SELECT TOP 5
            [transcript_id], [customer_id], [audio_filename],
            [loaded_at], LEFT([call_summary], 150) AS preview
        FROM [dbo].[CallTranscript]
        WHERE [transcript_status] = 'Loaded' AND [call_summary] IS NOT NULL
        ORDER BY [loaded_at] DESC
    """)
    print("\n--- Recent Loaded Transcripts ---")
    for row in cursor.fetchall():
        print(f"  ID: {row[0]} | Customer: {row[1]} | {row[2]}")
        print(f"    Loaded: {row[3]}")
        print(f"    Summary: {row[4]}...")
        print()

    cursor.close()
    conn.close()


# ============================================================
# RETRY
# ============================================================

def retry_not_found():
    logger.info("Retrying NotFound transcripts...")
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE [dbo].[CallTranscript]
        SET [transcript_status] = 'Pending',
            [loaded_at] = NULL,
            [transcript_json] = NULL,
            [call_summary] = NULL, [call_segment] = NULL, [call_product] = NULL,
            [call_issues_json] = NULL, [call_root_causes_json] = NULL,
            [customer_quotes_json] = NULL,
            [updated_at] = GETDATE()
        WHERE [transcript_status] = 'NotFound'
    """)
    reset_count = cursor.rowcount
    conn.commit()
    cursor.close()
    conn.close()

    logger.info(f"Reset {reset_count} NotFound records")
    if reset_count > 0:
        process_transcripts()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "verify":
            verify_results()
        elif cmd == "retry":
            retry_not_found()
        elif cmd == "link":
            conn = get_connection()
            n = link_events_to_transcripts(conn)
            print(f"Linked {n} events to transcripts")
            conn.close()
        elif cmd == "help":
            print("""
Customer 360 - Transcript Loader v2
====================================
    python load_transcripts_v2.py           Load all pending transcripts
    python load_transcripts_v2.py verify    Check status and extraction stats
    python load_transcripts_v2.py retry     Retry previously NotFound files
    python load_transcripts_v2.py link      Re-link events to transcripts
    python load_transcripts_v2.py help      Show this help
            """)
        else:
            print(f"Unknown: {cmd}. Use 'help'.")
    else:
        process_transcripts()
