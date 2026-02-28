"""
VIBO - Virtual Intelligence Briefing Officer
=================================================
RAG-based chatbot backend for Customer 360 data.

This package provides:
- Structured data retrieval via SQL tools (70% of queries)
- Semantic search via ChromaDB embeddings (30% of queries)
- 8 pre-built SQL tool functions for customer data
- Daily ETL pipeline for embeddings
- OpenAI function-calling schemas

Usage:
    from vibo.vibo_sql_tools import get_account_summary
    from vibo.vibo_vector_store import VectorStore

    # Get structured data
    summary = get_account_summary("10900099")

    # Semantic search
    store = VectorStore()
    results = store.search_customer_history(
        customer_id="10900099",
        query="customer mentioned switching to Sky"
    )

Components:
    - vibo_config.py: Configuration
    - vibo_database.py: SQL Server connection pool
    - vibo_sql_tools.py: 8 SQL retrieval functions
    - vibo_vector_store.py: ChromaDB wrapper
    - vibo_embedding_pipeline.py: ETL for embeddings
    - vibo_tool_registry.py: OpenAI function schemas
    - vibo_validate_foundation.py: Validation script
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

# Make key imports available at package level
from vibo.vibo_config import (
    DB_CONFIG,
    LLM_PROVIDER,
    EMBEDDING_PROVIDER,
    CHROMA_PERSIST_PATH,
    VECTOR_SEARCH_TOP_K,
)
from vibo.vibo_database import db_cursor
from vibo.vibo_sql_tools import (
    get_account_summary,
    get_open_cases,
    get_recent_calls,
    get_revenue_info,
    get_device_info,
    get_contact_history,
    get_risk_assessment,
    get_customer_voice,
)
from vibo.vibo_vector_store import VectorStore

__all__ = [
    # Config
    "DB_CONFIG",
    "LLM_PROVIDER",
    "EMBEDDING_PROVIDER",
    "CHROMA_PERSIST_PATH",
    "VECTOR_SEARCH_TOP_K",
    # Database
    "db_cursor",
    # SQL Tools
    "get_account_summary",
    "get_open_cases",
    "get_recent_calls",
    "get_revenue_info",
    "get_device_info",
    "get_contact_history",
    "get_risk_assessment",
    "get_customer_voice",
    # Vector Store
    "VectorStore",
]
