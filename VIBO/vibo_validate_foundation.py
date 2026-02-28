"""
VIBO Foundation Validator
=========================
Run this script to verify the entire VIBO foundation is correctly set up.
Tests: configuration, database connectivity, VIBO tables, embedding provider,
ChromaDB, SQL tools, and vector search.

Usage:
    python vibo_validate_foundation.py                 # Run all checks
    python vibo_validate_foundation.py --customer 10900099  # Test with specific customer
    python vibo_validate_foundation.py --skip-embed    # Skip embedding test (no Ollama needed)
"""

import argparse
import json
import sys
import time
from datetime import datetime

# ─── Results tracking ───
PASS = "\u2713"
FAIL = "\u2717"
WARN = "\u26A0"
results = {"passed": 0, "failed": 0, "warnings": 0}


def check(name: str, passed: bool, detail: str = "", warn: bool = False):
    """Record and print a check result."""
    if warn:
        results["warnings"] += 1
        print(f"  {WARN} {name}: {detail}")
    elif passed:
        results["passed"] += 1
        print(f"  {PASS} {name}" + (f": {detail}" if detail else ""))
    else:
        results["failed"] += 1
        print(f"  {FAIL} {name}" + (f": {detail}" if detail else ""))


def main():
    parser = argparse.ArgumentParser(description="VIBO Foundation Validator")
    parser.add_argument("--customer", type=str, default="10900099",
                       help="Customer ID for tool testing (default: 10900099)")
    parser.add_argument("--skip-embed", action="store_true",
                       help="Skip embedding provider test")
    args = parser.parse_args()
    
    print("=" * 70)
    print("VIBO FOUNDATION VALIDATION")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Test Customer: {args.customer}")
    print("=" * 70)
    
    # ─── 1. CONFIGURATION ─────────────────────────────────────────────────
    print("\n[1/6] Configuration")
    try:
        from vibo_config import (
            DB_CONFIG, LLM_PROVIDER, EMBEDDING_PROVIDER,
            CHROMA_PERSIST_PATH, AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT,
            OLLAMA_BASE_URL,
        )
        check("Config module loads", True)
        check("DB server configured", bool(DB_CONFIG.get("server")),
              DB_CONFIG.get("server", "NOT SET"))
        check("LLM provider set", bool(LLM_PROVIDER), LLM_PROVIDER)
        check("Embedding provider set", bool(EMBEDDING_PROVIDER), EMBEDDING_PROVIDER)
        check("ChromaDB path set", bool(CHROMA_PERSIST_PATH),
              CHROMA_PERSIST_PATH[:50])
        
        if LLM_PROVIDER == "azure_openai":
            check("Azure API key set", bool(AZURE_OPENAI_API_KEY),
                  f"***{AZURE_OPENAI_API_KEY[-4:]}" if AZURE_OPENAI_API_KEY else "MISSING")
            check("Azure endpoint set", bool(AZURE_OPENAI_ENDPOINT),
                  AZURE_OPENAI_ENDPOINT[:40] + "..." if AZURE_OPENAI_ENDPOINT else "MISSING")
        else:
            check("Ollama URL set", bool(OLLAMA_BASE_URL), OLLAMA_BASE_URL)
        
    except Exception as e:
        check("Config module loads", False, str(e))
        print("\n  Cannot proceed without configuration. Exiting.")
        sys.exit(1)
    
    # ─── 2. DATABASE CONNECTIVITY ──────────────────────────────────────────
    print("\n[2/6] Database Connectivity")
    try:
        from vibo_database import test_connection, db_cursor
        
        info = test_connection()
        connected = info.get("status") == "connected"
        check("SQL Server connection", connected,
              f"{info.get('server', '?')}/{info.get('database', '?')}" if connected else info.get("error", ""))
        
        if connected:
            # Check source tables
            source_tables = info.get("source_tables", [])
            required_tables = [
                "CallTranscript", "Customer360_Events",
                "LLM_Customer_Summary", "Revenue_Cache",
            ]
            for table in required_tables:
                check(f"Source table: {table}", table in source_tables,
                      "found" if table in source_tables else "MISSING")
            
            # Check optional tables
            if "Customer_Device_Assets" in source_tables:
                check("Source table: Customer_Device_Assets", True, "found")
            else:
                check("Source table: Customer_Device_Assets", False,
                      "MISSING (device tools will not work)", warn=True)
            
            # Check VIBO tables
            vibo_tables = info.get("vibo_tables", [])
            vibo_required = [
                "VIBO_Chat_Sessions", "VIBO_Chat_Messages",
                "VIBO_Embedding_Log", "VIBO_Feedback",
            ]
            for table in vibo_required:
                if table in vibo_tables:
                    check(f"VIBO table: {table}", True, "found")
                else:
                    check(f"VIBO table: {table}", False,
                          "MISSING - run vibo_schema.sql", warn=True)
            
            # Check row counts
            with db_cursor() as cursor:
                for table in ["CallTranscript", "Customer360_Events", "LLM_Customer_Summary"]:
                    if table in source_tables:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        check(f"Row count: {table}", count > 0, f"{count:,} rows")
        
    except Exception as e:
        check("Database connectivity", False, str(e))
    
    # ─── 3. EMBEDDING PROVIDER ─────────────────────────────────────────────
    print("\n[3/6] Embedding Provider")
    if args.skip_embed:
        print("  (skipped with --skip-embed)")
    else:
        try:
            from vibo_vector_store import EmbeddingProvider
            
            embedder = EmbeddingProvider()
            check("Embedding provider initialised", True, embedder.provider)
            
            start = time.time()
            emb = embedder.embed_single("Test embedding for VIBO validation")
            elapsed = time.time() - start
            
            check("Single embedding generated", len(emb) > 0,
                  f"dimension={len(emb)}, took {elapsed:.2f}s")
            
            # Test batch
            start = time.time()
            batch = embedder.embed(["Test one", "Test two", "Test three"])
            elapsed = time.time() - start
            
            check("Batch embedding (3 texts)", len(batch) == 3,
                  f"3 embeddings in {elapsed:.2f}s")
            
        except Exception as e:
            check("Embedding provider", False, str(e))
            if "Connection refused" in str(e) or "ConnectError" in str(e):
                print(f"\n  Hint: Make sure Ollama is running:")
                print(f"    ollama serve")
                print(f"    ollama pull nomic-embed-text")
    
    # ─── 4. CHROMADB ───────────────────────────────────────────────────────
    print("\n[4/6] ChromaDB Vector Store")
    try:
        from vibo_vector_store import VectorStore
        
        store = VectorStore()
        store.initialize()
        check("ChromaDB initialised", True)
        
        stats = store.get_stats()
        check("Transcripts collection", True,
              f"{stats['transcripts_count']:,} documents")
        check("Events collection", True,
              f"{stats['events_count']:,} documents")
        
        # Test upsert + search (if embedding is available and not skipped)
        if not args.skip_embed:
            # Insert test document
            test_docs = [{
                "id": "_vibo_test_doc",
                "text": "Customer called about broadband speed issues in Maynooth area",
                "metadata": {
                    "customer_id": "_TEST_",
                    "call_start": "2026-02-18T10:00:00",
                    "call_segment": "Broadband",
                    "call_product": "Fixed",
                    "source": "test",
                },
            }]
            upserted = store.upsert_transcripts(test_docs)
            check("Test document upsert", upserted == 1, f"{upserted} doc")
            
            # Search
            results_search = store.search_customer_history(
                customer_id="_TEST_", query="broadband speed problem"
            )
            found = results_search.get("total_results", 0) > 0
            check("Test vector search", found,
                  f"{results_search['total_results']} result(s)")
            
            if found and results_search["transcripts"]:
                sim = results_search["transcripts"][0].get("similarity")
                check("Similarity score reasonable", sim and sim > 0.5,
                      f"similarity={sim}")
            
            # Cleanup test doc
            store.transcripts_collection.delete(ids=["_vibo_test_doc"])
            check("Test document cleaned up", True)
    
    except Exception as e:
        check("ChromaDB", False, str(e))
    
    # ─── 5. SQL TOOLS ──────────────────────────────────────────────────────
    print(f"\n[5/6] SQL Tools (customer: {args.customer})")
    try:
        from vibo_sql_tools import SQL_TOOLS
        
        check("SQL tools module loads", True, f"{len(SQL_TOOLS)} tools registered")
        
        for name, func in SQL_TOOLS.items():
            try:
                start = time.time()
                result = func(args.customer)
                elapsed = time.time() - start
                
                # Check if we got meaningful data
                has_data = False
                if isinstance(result, dict):
                    has_data = result.get("found", True)  # default True if no 'found' key
                
                detail = f"{elapsed:.2f}s"
                if isinstance(result, dict):
                    if "total_calls_returned" in result:
                        detail += f", {result['total_calls_returned']} calls"
                    elif "total_interactions" in result:
                        detail += f", {result['total_interactions']} interactions"
                    elif "total_devices" in result:
                        detail += f", {result['total_devices']} devices"
                    elif "open_cases_count" in result:
                        detail += f", {result['open_cases_count']} open cases"
                
                if has_data:
                    check(f"Tool: {name}()", True, detail)
                else:
                    check(f"Tool: {name}()", False,
                          f"No data for customer {args.customer}", warn=True)
                    
            except Exception as e:
                check(f"Tool: {name}()", False, str(e)[:80])
    
    except Exception as e:
        check("SQL tools module", False, str(e))
    
    # ─── 6. TOOL REGISTRY ─────────────────────────────────────────────────
    print("\n[6/6] Tool Registry")
    try:
        from vibo_tool_registry import VIBO_TOOLS, QUICK_ACTIONS, get_tool_names
        
        tool_names = get_tool_names()
        check("Tool registry loads", True, f"{len(VIBO_TOOLS)} tool definitions")
        check("Quick actions defined", True, f"{len(QUICK_ACTIONS)} actions")
        
        # Verify every SQL tool has a matching registry definition
        from vibo_sql_tools import SQL_TOOLS as sql_tools
        for sql_name in sql_tools.keys():
            has_def = sql_name in tool_names
            if not has_def:
                # search_customer_history is vector, not SQL - that's OK
                if sql_name not in ("search_customer_history",):
                    check(f"Registry entry for {sql_name}", False,
                          "SQL tool has no LLM schema definition", warn=True)
        
        # Verify search_customer_history is in registry
        check("search_customer_history in registry",
              "search_customer_history" in tool_names)
        
        # Validate tool schemas
        for tool in VIBO_TOOLS:
            fn = tool.get("function", {})
            name = fn.get("name", "?")
            has_required = "required" in fn.get("parameters", {})
            has_desc = bool(fn.get("description"))
            has_customer_id = "customer_id" in fn.get("parameters", {}).get("properties", {})
            
            if not has_desc:
                check(f"Schema: {name}", False, "missing description")
            elif not has_customer_id:
                check(f"Schema: {name}", False, "missing customer_id parameter")
            else:
                check(f"Schema: {name}", True, "valid")
    
    except Exception as e:
        check("Tool registry", False, str(e))
    
    # ─── SUMMARY ───────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    total = results["passed"] + results["failed"] + results["warnings"]
    print(f"RESULTS: {results['passed']}/{total} passed, "
          f"{results['failed']} failed, {results['warnings']} warnings")
    
    if results["failed"] == 0:
        print("\n  VIBO foundation is ready! You can now build the API layer on top.")
    else:
        print(f"\n  {results['failed']} check(s) failed. Fix these before proceeding.")
    
    if results["warnings"] > 0:
        print(f"  {results['warnings']} warning(s) - non-blocking but worth addressing.")
    
    print("=" * 70)
    
    return 0 if results["failed"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
