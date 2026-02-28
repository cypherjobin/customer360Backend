"""
VIBO FastAPI Server
====================
REST API endpoints for Junaid's chat widget frontend.

Run this server:
    python vibo_api.py

Then access:
    - http://localhost:8000/docs - Interactive API documentation
    - http://localhost:8000/customer/{customer_id}/summary - Get customer summary
    - http://localhost:8000/customer/{customer_id}/search?q={query} - Semantic search
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import logging

# VIBO imports
from vibo_config import (
    LLM_PROVIDER, EMBEDDING_PROVIDER,
    CHROMA_PERSIST_PATH, VECTOR_SEARCH_TOP_K
)
from vibo_database import test_connection
from vibo_vector_store import VectorStore
from vibo_sql_tools import (
    get_account_summary,
    get_open_cases,
    get_recent_calls,
    get_revenue_and_products,
    get_device_portfolio,
    get_interactions,
    get_risk_assessment,
    get_contact_timeline,
)
from vibo_llm import chat_completion, build_rag_prompt

# Setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("vibo.api")

app = FastAPI(
    title="VIBO Customer 360 API",
    description="RAG-based chatbot backend for customer data",
    version="1.0.0"
)

# Enable CORS for Junaid's frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify Junaid's frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize vector store (singleton)
_vector_store: Optional[VectorStore] = None

def get_vector_store() -> VectorStore:
    """Get or create vector store singleton."""
    global _vector_store
    if _vector_store is None:
        logger.info("Initializing VectorStore...")
        _vector_store = VectorStore()
        _vector_store.initialize()
        logger.info(f"VectorStore initialized: {_vector_store.get_stats()}")
    return _vector_store


# ─── RESPONSE MODELS ───────────────────────────────────────────────────────

class CustomerSummary(BaseModel):
    """Customer account summary with AI-generated overview."""
    customer_id: str
    summary_json: Optional[Dict[str, Any]] = None
    account_value: Optional[Dict[str, Any]] = None
    risk_assessment: Optional[Dict[str, Any]] = None
    open_cases: List[Dict[str, Any]] = []
    recent_calls: List[Dict[str, Any]] = []
    devices: List[Dict[str, Any]] = []
    revenue_info: Optional[Dict[str, Any]] = None


class SearchResult(BaseModel):
    """Semantic search result."""
    id: str
    text: str
    metadata: Dict[str, Any]
    similarity: float


class SearchResponse(BaseModel):
    """Semantic search response."""
    customer_id: str
    query: str
    transcripts: List[SearchResult]
    events: List[SearchResult]
    total_results: int


class HealthResponse(BaseModel):
    """API health check response."""
    status: str
    database: Dict[str, Any]
    vector_store: Dict[str, Any]
    config: Dict[str, Any]


class ChatRequest(BaseModel):
    """Request for conversational Q&A about a customer."""
    question: str
    include_context: bool = False  # If True, return retrieved context along with answer
    # customer_id comes from URL path, not request body


class ChatResponse(BaseModel):
    """Response from conversational Q&A."""
    customer_id: str
    question: str
    answer: str
    sources: List[str] = []
    context: Optional[str] = None  # Only included if include_context=True


# ─── ENDPOINTS ─────────────────────────────────────────────────────────────

@app.get("/", tags=["Root"])
def root():
    """Root endpoint with API information."""
    return {
        "name": "VIBO Customer 360 API",
        "version": "1.0.0",
        "docs": "/docs",
        "endpoints": {
            "health": "/health",
            "customer_summary": "/customer/{customer_id}/summary",
            "customer_cases": "/customer/{customer_id}/cases",
            "customer_calls": "/customer/{customer_id}/calls",
            "customer_devices": "/customer/{customer_id}/devices",
            "customer_revenue": "/customer/{customer_id}/revenue",
            "customer_risk": "/customer/{customer_id}/risk",
            "customer_interactions": "/customer/{customer_id}/interactions",
            "search": "/customer/{customer_id}/search",
        }
    }


@app.get("/health", response_model=HealthResponse, tags=["Health"])
def health_check():
    """
    Check API health and configuration.

    Returns database connectivity, vector store status, and configuration.
    """
    # Check database
    db_info = test_connection()

    # Check vector store
    try:
        store = get_vector_store()
        vs_stats = store.get_stats()
        vs_info = {"status": "connected", **vs_stats}
    except Exception as e:
        vs_info = {"status": "error", "error": str(e)}

    return HealthResponse(
        status="healthy" if db_info["status"] == "connected" else "degraded",
        database=db_info,
        vector_store=vs_info,
        config={
            "llm_provider": LLM_PROVIDER,
            "embedding_provider": EMBEDDING_PROVIDER,
            "chroma_path": CHROMA_PERSIST_PATH,
            "vector_top_k": VECTOR_SEARCH_TOP_K,
        }
    )


@app.get("/customer/{customer_id}/summary", response_model=CustomerSummary, tags=["Customer"])
def get_customer_summary_endpoint(customer_id: str):
    """
    Get complete customer summary including AI-generated overview.

    This is the main endpoint for the chat widget - it returns:
    - AI-generated account summary
    - Risk assessment (churn risk, health score)
    - Open cases (Pega + ServiceNow)
    - Recent calls with transcripts
    - Device information
    - Revenue details
    """
    try:
        return CustomerSummary(
            customer_id=customer_id,
            summary_json=get_account_summary(customer_id),
            risk_assessment=get_risk_assessment(customer_id),
            open_cases=get_open_cases(customer_id).get("cases", []),
            recent_calls=get_recent_calls(customer_id).get("calls", []),
            devices=get_device_portfolio(customer_id).get("devices", []),
            revenue_info=get_revenue_and_products(customer_id),
        )
    except Exception as e:
        logger.error(f"Error getting summary for {customer_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customer/{customer_id}/cases", tags=["Customer"])
def get_customer_cases(customer_id: str):
    """Get open cases for a customer (Pega + ServiceNow)."""
    try:
        return {"customer_id": customer_id, "cases": get_open_cases(customer_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customer/{customer_id}/calls", tags=["Customer"])
def get_customer_calls(customer_id: str, limit: int = Query(5, ge=1, le=20)):
    """Get recent call recordings for a customer."""
    try:
        result = get_recent_calls(customer_id, limit=limit)
        return {"customer_id": customer_id, **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customer/{customer_id}/devices", tags=["Customer"])
def get_customer_devices(customer_id: str):
    """Get device assets and financing details for a customer."""
    try:
        result = get_device_portfolio(customer_id)
        return {"customer_id": customer_id, **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customer/{customer_id}/revenue", tags=["Customer"])
def get_customer_revenue(customer_id: str):
    """Get revenue information for a customer."""
    try:
        result = get_revenue_and_products(customer_id)
        return {"customer_id": customer_id, **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customer/{customer_id}/risk", tags=["Customer"])
def get_customer_risk(customer_id: str):
    """Get risk assessment for a customer (churn risk, health score)."""
    try:
        return {"customer_id": customer_id, "risk": get_risk_assessment(customer_id)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customer/{customer_id}/search", response_model=SearchResponse, tags=["Search"])
def search_customer_history(
    customer_id: str,
    q: str = Query(..., description="Natural language search query"),
    top_k: int = Query(5, ge=1, le=20, description="Number of results to return"),
    source: str = Query("all", description="Search source: all, transcripts, or events")
):
    """
    Semantic search across customer's conversation history.

    Examples:
        - /customer/10900099/search?q=billing%20issue
        - /customer/10900099/search?q=customer%20mentioned%20canceling
        - /customer/10900099/search?q=slow%20internet&source=transcripts
    """
    try:
        store = get_vector_store()
        results = store.search_customer_history(
            customer_id=customer_id,
            query=q,
            top_k=top_k,
            source=source
        )

        # Convert to response models
        transcripts = [
            SearchResult(**r) for r in results.get("transcripts", [])
        ]
        events = [
            SearchResult(**r) for r in results.get("events", [])
        ]

        return SearchResponse(
            customer_id=customer_id,
            query=q,
            transcripts=transcripts,
            events=events,
            total_results=results.get("total_results", 0)
        )
    except Exception as e:
        logger.error(f"Search error for {customer_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/customer/{customer_id}/interactions", tags=["Customer"])
def get_customer_interactions(customer_id: str, days_back: int = Query(30, ge=1, le=365)):
    """Get recent interactions for a customer."""
    try:
        result = get_interactions(customer_id, days_back=days_back)
        return {"customer_id": customer_id, **result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/customer/{customer_id}/chat", response_model=ChatResponse, tags=["Chat"])
def chat_with_customer(customer_id: str, request: ChatRequest):
    """
    Conversational Q&A about a customer's history.

    This endpoint uses RAG (Retrieval-Augmented Generation) to answer
    natural language questions about a customer using their past
    interactions, calls, and case notes.

    Examples:
        - "Where was the customer roaming and what was the issue?"
        - "Has this customer complained about billing before?"
        - "What devices does the customer have?"

    The answer is based ONLY on the customer's actual history in the system.
    """
    try:
        # Step 1: Retrieve relevant context via semantic search
        store = get_vector_store()
        search_results = store.search_customer_history(
            customer_id=customer_id,
            query=request.question,
            top_k=5,
            source="all"
        )

        # Step 2: Build context from search results
        context_parts = []
        sources = []

        for result in search_results.get("transcripts", []):
            meta = result["metadata"]
            context_parts.append(f"[Call] {result['text']}")
            sources.append(f"transcript:{result['id']}")

        for result in search_results.get("events", []):
            meta = result["metadata"]
            event_type = meta.get("event_type", "Unknown")
            source_system = meta.get("source_system", "Unknown")
            timestamp = meta.get("event_timestamp", "Unknown date")
            context_parts.append(f"[{event_type} - {source_system} on {timestamp}] {result['text']}")
            sources.append(f"{source_system}:{result['id']}")

        context = "\n\n".join(context_parts) if context_parts else "No relevant context found."

        # Step 3: Get customer structured data for additional context
        customer_summary = None
        try:
            customer_summary = {
                "devices": get_device_portfolio(customer_id).get("devices", []),
                "revenue_info": get_revenue_and_products(customer_id),
                "open_cases": get_open_cases(customer_id).get("cases", []),
            }
        except:
            pass  # Structured data is optional

        # Step 4: Build RAG prompt
        messages = build_rag_prompt(
            question=request.question,
            context=context,
            customer_summary=customer_summary
        )

        # Step 5: Get LLM response
        answer = chat_completion(messages)

        return ChatResponse(
            customer_id=customer_id,
            question=request.question,
            answer=answer,
            sources=sources,
            context=context if request.include_context else None
        )
    except Exception as e:
        logger.error(f"Chat error for {customer_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ─── RUN SERVER ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    print("""
    ================================================================
                        VIBO API Server
    ================================================================
      API Documentation:  http://localhost:8000/docs
      Health Check:       http://localhost:8000/health

      Example endpoints:
        GET /customer/10900099/summary
        GET /customer/10900099/search?q=billing%20issue
        GET /customer/10900099/cases
    ================================================================
    """)

    # Initialize vector store at startup
    get_vector_store()

    uvicorn.run(
        app,
        host="0.0.0.0",  # Allow external connections (Junaid can connect)
        port=8000,
        log_level="info"
    )
