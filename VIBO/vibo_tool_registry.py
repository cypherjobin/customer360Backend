"""
VIBO Tool Registry
==================
Defines tool schemas in OpenAI function-calling format.
Compatible with both Azure OpenAI and Ollama (via tool-use).

These definitions tell the LLM what tools are available,
what each tool does, and what parameters they accept.
"""

# ═══════════════════════════════════════════════════════════════════════════════
# TOOL DEFINITIONS (OpenAI function-calling format)
# ═══════════════════════════════════════════════════════════════════════════════

VIBO_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "get_account_summary",
            "description": (
                "Retrieve the complete AI-generated customer summary including "
                "health score, churn risk, customer effort score (CES), sentiment analysis, "
                "agent briefing, recommended actions, contact timeline, and account value details. "
                "Use this for broad questions about the customer or when asked to 'summarise the account'."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "The customer ID to look up",
                    }
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_open_cases",
            "description": (
                "Get all open and unresolved cases and incidents from Pega and ServiceNow "
                "for the customer. Returns case IDs, type, status, priority, assigned team, "
                "SLA breach status, and description. Set include_resolved=true to also see "
                "recently closed cases."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "The customer ID",
                    },
                    "include_resolved": {
                        "type": "boolean",
                        "description": "If true, also returns resolved/closed cases. Default: false",
                        "default": False,
                    },
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_recent_calls",
            "description": (
                "Retrieve recent call recordings and transcripts with AI-generated summaries, "
                "detected issues, root causes, and customer quotes. Use this when the agent asks "
                "about recent calls, why the customer called, or what was discussed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "The customer ID",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of calls to return (1-20). Default: 5",
                        "default": 5,
                        "minimum": 1,
                        "maximum": 20,
                    },
                    "days_back": {
                        "type": "integer",
                        "description": "How many days back to search. Default: 30",
                        "default": 30,
                    },
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_revenue_and_products",
            "description": (
                "Get customer revenue details, product portfolio, contract dates, tenure, "
                "service status, and revenue segmentation (High/Medium/Low Value). "
                "Includes mobile and fixed revenue breakdown, device financing, and plan count."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "The customer ID",
                    },
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_device_portfolio",
            "description": (
                "Get all devices associated with the customer including brand, model, memory, "
                "IMEI, contract status, installment details, remaining payments, and monthly "
                "instalment charge (MIC). Set active_only=true to see only active contracts."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "The customer ID",
                    },
                    "active_only": {
                        "type": "boolean",
                        "description": "If true, only returns devices with active contracts. Default: false",
                        "default": False,
                    },
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_interactions",
            "description": (
                "Get all customer interactions across all source systems (Interaction, "
                "CallRecording, Pega, ServiceNow). Returns event type, timestamp, source, "
                "and detail summary. Can be filtered by source system."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "The customer ID",
                    },
                    "days_back": {
                        "type": "integer",
                        "description": "How many days back to search. Default: 30",
                        "default": 30,
                    },
                    "source_system": {
                        "type": "string",
                        "description": "Optional filter: 'Interaction', 'CallRecording', 'Pega', or 'ServiceNow'",
                        "enum": ["Interaction", "CallRecording", "Pega", "ServiceNow"],
                    },
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_risk_assessment",
            "description": (
                "Get comprehensive risk assessment including health score and band, "
                "churn risk with specific indicators, escalation risk with score and reason, "
                "customer effort score, current sentiment, SLA breach status, and "
                "recommended actions. Use this for risk-related questions."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "The customer ID",
                    },
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_contact_timeline",
            "description": (
                "Get the chronological timeline of all customer contacts in the last 30 days, "
                "including call intent summaries and source system breakdowns. Use this when "
                "asked about the sequence of events or contact history."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "The customer ID",
                    },
                },
                "required": ["customer_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_customer_history",
            "description": (
                "Semantic search across the customer's call transcripts and interaction history. "
                "Use this when the agent asks about specific topics, keywords, or mentions that "
                "may appear in unstructured conversation text. For example: 'Did the customer "
                "ever mention switching to Sky?', 'Any complaints about broadband speed?', "
                "'What did they say about billing?'. This searches through the actual call "
                "transcript content and event notes using AI-powered similarity matching."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "customer_id": {
                        "type": "string",
                        "description": "The customer ID to search within",
                    },
                    "query": {
                        "type": "string",
                        "description": "Natural language search query describing what to find",
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Number of most relevant results to return. Default: 5",
                        "default": 5,
                        "minimum": 1,
                        "maximum": 10,
                    },
                },
                "required": ["customer_id", "query"],
            },
        },
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# QUICK ACTION DEFINITIONS
# ═══════════════════════════════════════════════════════════════════════════════
# These map the UI quick-action buttons to specific tool calls,
# bypassing the LLM's tool-selection reasoning for faster responses.

QUICK_ACTIONS = {
    "summarize_account": {
        "label": "Summarize account",
        "icon": "📊",
        "tool": "get_account_summary",
        "params": {},  # customer_id added at runtime
        "system_instruction": (
            "Generate a comprehensive account briefing. Include: customer type, "
            "revenue segment, health status, any open issues, sentiment, and "
            "recommended next actions. Be concise but thorough."
        ),
    },
    "open_issues": {
        "label": "Open issues",
        "icon": "🔴",
        "tool": "get_open_cases",
        "params": {"include_resolved": False},
        "system_instruction": (
            "List all open and unresolved cases. For each, include the case ID, "
            "type, status, which team it's assigned to, and how long it's been open. "
            "Highlight any SLA breaches."
        ),
    },
    "last_call_reason": {
        "label": "Last call reason",
        "icon": "📞",
        "tool": "get_recent_calls",
        "params": {"limit": 1},
        "system_instruction": (
            "Explain why the customer last contacted us. Include the call date, "
            "duration, what was discussed, any issues identified, and the outcome. "
            "Include any relevant customer quotes."
        ),
    },
    "risk_assessment": {
        "label": "Risk assessment",
        "icon": "⚠️",
        "tool": "get_risk_assessment",
        "params": {},
        "system_instruction": (
            "Provide a risk assessment. Include health score and band, churn risk "
            "with specific indicators, escalation risk, customer effort score, "
            "and current sentiment. Highlight the most critical risks and "
            "recommended actions."
        ),
    },
}


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════════

def get_tool_names() -> list[str]:
    """Get list of all available tool names."""
    return [t["function"]["name"] for t in VIBO_TOOLS]


def get_tool_by_name(name: str) -> dict:
    """Get a tool definition by name."""
    for tool in VIBO_TOOLS:
        if tool["function"]["name"] == name:
            return tool
    return None


if __name__ == "__main__":
    print("VIBO Tool Registry")
    print("=" * 60)
    print(f"\nRegistered tools ({len(VIBO_TOOLS)}):")
    for tool in VIBO_TOOLS:
        fn = tool["function"]
        params = fn["parameters"]["properties"]
        required = fn["parameters"].get("required", [])
        print(f"\n  {fn['name']}({', '.join(required)})")
        print(f"    {fn['description'][:100]}...")
        print(f"    Parameters: {list(params.keys())}")
    
    print(f"\nQuick actions ({len(QUICK_ACTIONS)}):")
    for key, action in QUICK_ACTIONS.items():
        print(f"  {action['icon']} {action['label']} -> {action['tool']}()")
