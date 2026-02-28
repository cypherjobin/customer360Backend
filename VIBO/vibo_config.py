"""
VIBO Configuration
==================
Centralised configuration for all VIBO components.
Loads from .env file in the Customer360 root directory.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ─── Load .env from Customer360 root or config folder ───
_env_path_root = Path(__file__).resolve().parent.parent / ".env"
_env_path_config = Path(__file__).resolve().parent.parent / "config" / ".env"
if _env_path_root.exists():
    load_dotenv(_env_path_root)
elif _env_path_config.exists():
    load_dotenv(_env_path_config)
else:
    load_dotenv()  # fallback to cwd


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════════════════════════
DB_CONFIG = {
    "server":             os.getenv("VIBO_DB_SERVER", "DBUATL01"),
    "database":           os.getenv("VIBO_DB_NAME", "Customer_FeedBack_JIT"),
    "driver":             os.getenv("VIBO_DB_DRIVER", "{ODBC Driver 18 for SQL Server}"),
    "user":               os.getenv("VIBO_DB_USER", ""),
    "password":           os.getenv("VIBO_DB_PASSWORD", ""),
    "trusted_connection": os.getenv("VIBO_DB_TRUSTED", "no"),
}

# Build connection string with SQL Server or Windows authentication
if DB_CONFIG["user"] and DB_CONFIG["password"]:
    # SQL Server authentication
    DB_CONN_STRING = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"UID={DB_CONFIG['user']};"
        f"PWD={DB_CONFIG['password']};"
        f"TrustServerCertificate=yes;"
    )
else:
    # Windows authentication
    DB_CONN_STRING = (
        f"DRIVER={DB_CONFIG['driver']};"
        f"SERVER={DB_CONFIG['server']};"
        f"DATABASE={DB_CONFIG['database']};"
        f"Trusted_Connection={DB_CONFIG['trusted_connection']};"
        f"TrustServerCertificate=yes;"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# LLM PROVIDER (for chat)
# ═══════════════════════════════════════════════════════════════════════════════
LLM_PROVIDER = os.getenv("VIBO_LLM_PROVIDER", "azure_openai")   # azure_openai | ollama

# Azure OpenAI (reuses existing Customer360 .env keys)
AZURE_OPENAI_API_KEY    = os.getenv("AZURE_OPENAI_API_KEY", "")
AZURE_OPENAI_ENDPOINT   = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_OPENAI_DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
AZURE_OPENAI_API_VERSION = os.getenv("AZURE_OPENAI_API_VERSION", "2024-06-01")

# Ollama (local)
OLLAMA_BASE_URL = os.getenv("VIBO_OLLAMA_URL", "http://localhost:11434")
OLLAMA_CHAT_MODEL = os.getenv("VIBO_OLLAMA_CHAT_MODEL", "llama3.1:8b")


# ═══════════════════════════════════════════════════════════════════════════════
# EMBEDDING PROVIDER
# ═══════════════════════════════════════════════════════════════════════════════
EMBEDDING_PROVIDER = os.getenv("VIBO_EMBEDDING_PROVIDER", "ollama")  # ollama | azure_openai

# Ollama embedding
OLLAMA_EMBED_MODEL = os.getenv("VIBO_OLLAMA_EMBED_MODEL", "nomic-embed-text")

# Azure embedding (if using Azure instead)
AZURE_EMBED_DEPLOYMENT = os.getenv("VIBO_AZURE_EMBED_DEPLOYMENT", "text-embedding-3-small")


# ═══════════════════════════════════════════════════════════════════════════════
# CHROMADB (Vector Store)
# ═══════════════════════════════════════════════════════════════════════════════
CHROMA_PERSIST_PATH = os.getenv("VIBO_CHROMA_PATH", str(Path(__file__).resolve().parent / "chromadb_data"))
CHROMA_HOST         = os.getenv("VIBO_CHROMA_HOST", "")        # empty = use local persistent
CHROMA_PORT         = int(os.getenv("VIBO_CHROMA_PORT", "8000"))

# Collection names
CHROMA_COLLECTION_TRANSCRIPTS = "vmi_llm_summaries"  # Changed: Now stores LLM summaries
CHROMA_COLLECTION_EVENTS      = "vmi_customer_events"


# ═══════════════════════════════════════════════════════════════════════════════
# EMBEDDING PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════
EMBED_BATCH_SIZE     = int(os.getenv("VIBO_EMBED_BATCH_SIZE", "100"))
EMBED_MAX_TEXT_LENGTH = int(os.getenv("VIBO_EMBED_MAX_TEXT", "2000"))  # chars per chunk


# ═══════════════════════════════════════════════════════════════════════════════
# SESSION / CONVERSATION
# ═══════════════════════════════════════════════════════════════════════════════
SESSION_TTL_MINUTES      = int(os.getenv("VIBO_SESSION_TTL_MINUTES", "60"))
MAX_HISTORY_MESSAGES     = int(os.getenv("VIBO_MAX_HISTORY_MESSAGES", "10"))
MAX_HISTORY_TOKENS       = int(os.getenv("VIBO_MAX_HISTORY_TOKENS", "4000"))
VECTOR_SEARCH_TOP_K      = int(os.getenv("VIBO_VECTOR_TOP_K", "5"))


# ═══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════════════════════
LOG_LEVEL = os.getenv("VIBO_LOG_LEVEL", "INFO")


def print_config():
    """Print current configuration (masking secrets)."""
    print("=" * 60)
    print("VIBO Configuration")
    print("=" * 60)
    print(f"  DB Server:          {DB_CONFIG['server']}")
    print(f"  DB Name:            {DB_CONFIG['database']}")
    print(f"  LLM Provider:       {LLM_PROVIDER}")
    if LLM_PROVIDER == "azure_openai":
        print(f"  Azure Endpoint:     {AZURE_OPENAI_ENDPOINT[:40]}..." if AZURE_OPENAI_ENDPOINT else "  Azure Endpoint:     NOT SET")
        print(f"  Azure Deployment:   {AZURE_OPENAI_DEPLOYMENT}")
        print(f"  Azure API Key:      {'***' + AZURE_OPENAI_API_KEY[-4:] if AZURE_OPENAI_API_KEY else 'NOT SET'}")
    else:
        print(f"  Ollama URL:         {OLLAMA_BASE_URL}")
        print(f"  Ollama Chat Model:  {OLLAMA_CHAT_MODEL}")
    print(f"  Embedding Provider: {EMBEDDING_PROVIDER}")
    print(f"  Embedding Model:    {OLLAMA_EMBED_MODEL if EMBEDDING_PROVIDER == 'ollama' else AZURE_EMBED_DEPLOYMENT}")
    print(f"  ChromaDB Path:      {CHROMA_PERSIST_PATH}")
    print(f"  ChromaDB Host:      {CHROMA_HOST or '(local persistent)'}")
    print(f"  Embed Batch Size:   {EMBED_BATCH_SIZE}")
    print(f"  Vector Top-K:       {VECTOR_SEARCH_TOP_K}")
    print(f"  Session TTL:        {SESSION_TTL_MINUTES} min")
    print(f"  Log Level:          {LOG_LEVEL}")
    print("=" * 60)


if __name__ == "__main__":
    print_config()
