"""
VIBO LLM Client
===============
Simple interface for Azure OpenAI chat completions.
Used by the /chat endpoint for conversational Q&A.
"""

import logging
from openai import AzureOpenAI
from vibo_config import (
    LLM_PROVIDER,
    AZURE_OPENAI_API_KEY, AZURE_OPENAI_ENDPOINT,
    AZURE_OPENAI_DEPLOYMENT, AZURE_OPENAI_API_VERSION,
    OLLAMA_BASE_URL, OLLAMA_CHAT_MODEL,
)

logger = logging.getLogger("vibo.llm")


def get_llm_client():
    """Get the appropriate LLM client based on configuration."""
    if LLM_PROVIDER == "azure_openai":
        if not AZURE_OPENAI_API_KEY or not AZURE_OPENAI_ENDPOINT:
            raise ValueError("Azure OpenAI credentials not configured. Check your .env file.")
        return AzureOpenAI(
            api_key=AZURE_OPENAI_API_KEY,
            api_version=AZURE_OPENAI_API_VERSION,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
        )
    else:
        raise ValueError(f"LLM provider {LLM_PROVIDER} not supported. Please set VIBO_LLM_PROVIDER=azure_openai")


def chat_completion(messages: list[dict], model: str = None) -> str:
    """
    Simple chat completion interface using Azure OpenAI.

    Args:
        messages: List of message dicts with 'role' and 'content'
        model: Optional model override

    Returns:
        The assistant's response text
    """
    client = get_llm_client()
    model = model or AZURE_OPENAI_DEPLOYMENT

    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.3,  # Lower temperature for more factual answers
            max_tokens=1000,
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"Azure OpenAI chat completion failed: {e}")
        raise


def build_rag_prompt(question: str, context: str, customer_summary: dict = None) -> list[dict]:
    """
    Build a RAG prompt with STRICT grounding in Virgin Media data.

    CRITICAL: The LLM MUST answer ONLY from the provided context.
    Do NOT use external knowledge or pre-trained information.

    Args:
        question: The user's question
        context: Retrieved context from vector search
        customer_summary: Optional customer structured data

    Returns:
        List of messages for the chat completion
    """
    # Build customer context section
    customer_context = ""
    if customer_summary:
        parts = []
        if customer_summary.get("devices"):
            devices = customer_summary["devices"]
            parts.append(f"- Devices: {len(devices)} device(s)")
            for d in devices[:3]:
                parts.append(f"  * {d.get('brand', 'N/A')} {d.get('model', 'N/A')}")
        if customer_summary.get("revenue_info"):
            r = customer_summary["revenue_info"]
            parts.append(f"- Revenue: {r.get('revenue', {}).get('monthly_total', 0)} per month")
            parts.append(f"- Segment: {r.get('revenue', {}).get('revenue_segment', 'N/A')}")
        if customer_summary.get("open_cases"):
            cases = customer_summary["open_cases"]
            parts.append(f"- Open Cases: {len(cases)} case(s)")
        if parts:
            customer_context = "\n".join(parts)

    # Build context section
    context_section = context if context else "No specific context found."
    customer_section = ""
    if customer_context:
        customer_section = f"\n\nCUSTOMER SUMMARY:\n{customer_context}"

    system_prompt = f"""You are a Virgin Media customer service assistant helping agents find information about customers.

CRITICAL INSTRUCTIONS - READ CAREFULLY:
1. You MUST answer ONLY using the provided Virgin Media customer data below
2. DO NOT use any external knowledge, pre-trained information, or make assumptions
3. If the answer is NOT in the provided context, say: "This information is not available in the customer's history"
4. DO NOT invent, guess, or infer information not present in the context
5. Stick strictly to what the transcripts, notes, and case details say

VIRGIN MEDIA CUSTOMER DATA:
{context_section}{customer_section}

ANSWER GUIDELINES:
- Be concise and factual based ONLY on the provided data
- Cite specific details when available (dates, locations, issue descriptions)
- If multiple events are mentioned, summarize them chronologically
- Use exact quotes from transcripts when relevant
- If you don't know the answer from the context, admit it clearly"""

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": question},
    ]


if __name__ == "__main__":
    import sys
    import io
    # Fix Windows console encoding
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

    # Test the LLM connection
    print("Testing VIBO LLM connection...")
    try:
        response = chat_completion([
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Say 'VIBO LLM is working!' without any emojis"}
        ])
        print(f"Response: {response}")
    except Exception as e:
        print(f"Error: {e}")
