"""
Customer 360 - LLM Summariser v4 (Enterprise)
================================================
Production-grade incremental summarisation.
Prompt: v6.0-enterprise

BUG FIXES IN v4:
  1. Fixed delta payload truncation logic (was building combined BEFORE truncating)
  2. Fixed double conn.close() in retry_all()
  3. Added STUCK_TIMEOUT_MINUTES constant (removed hardcode)
  4. Fixed watermark date comparison (handles both datetime and date types)
  5. Fixed scenario handling to match spec exactly

ENHANCEMENTS OVER v2:
  1. Deterministic window alignment (--run-date parameter)
  2. Processing state machine (PENDING → IN_PROGRESS → COMPLETED/FAILED)
  3. Revenue from Revenue_Cache (populated by refresh_revenue_cache.py)
  4. Exponential backoff retry (429, 500, timeouts)
  5. Token & cost governance (LLM_Token_Log, per-call capture)
  6. Deterministic escalation scoring (Python, not LLM)
  7. Periodic rebuild governance (forced every 30 days)
  8. Run logging (LLM_Run_Log)
  9. Concurrency readiness (--worker-id, atomic claim via sp_ClaimSummaryBatch)

SCENARIOS:
  1. FULL       — No existing summary OR empty summary → full 30d window → INSERT
  2. INCREMENTAL — Valid watermark within window → delta only → merge with existing → UPDATE
  3. REBUILD    — Stale watermark OR >30d since last full → full 30d → UPDATE

PREREQUISITES:
    pip install pyodbc openai python-dotenv

    Required DB objects (from phase2_enterprise_schema.sql):
        Revenue_Cache, LLM_Token_Log, LLM_Run_Log
        sp_ClaimSummaryBatch, sp_MarkSummaryCompleted,
        sp_MarkSummaryFailed, sp_ResetStuckSummaries,
        sp_SeedPendingSummaries, vw_CustomersPendingSummary

USAGE:
    python llm_summariser_v4.py --run-date 2026-02-17
    python llm_summariser_v4.py --run-date 2026-02-17 --worker-id A --batch-size 20
    python llm_summariser_v4.py status
    python llm_summariser_v4.py retry
    python llm_summariser_v4.py customer 12345 --run-date 2026-02-17
    python llm_summariser_v4.py help
"""

import pyodbc
import json
import os
import sys
import logging
import time
import random
import argparse
import socket
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

from openai import AzureOpenAI
import llm_enrichment


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

_api_key = os.getenv("AZURE_OPENAI_API_KEY")
_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
_deployment = os.getenv("AZURE_OPENAI_DEPLOYMENT")
_version = os.getenv("AZURE_OPENAI_API_VERSION")

if not _api_key or not _endpoint or not _deployment:
    print(f"\nERROR: Missing Azure OpenAI config")
    print(f"  AZURE_OPENAI_API_KEY:        {'SET' if _api_key else 'NOT SET'}")
    print(f"  AZURE_OPENAI_ENDPOINT:       {'SET' if _endpoint else 'NOT SET'}")
    print(f"  AZURE_OPENAI_DEPLOYMENT:     {'SET' if _deployment else 'NOT SET'}")
    print(f"  .env file: {os.path.abspath('.env')}")
    sys.exit(1)

DB_CONFIG = {
    "server": "DBUATL01",
    "database": "Customer_FeedBack_JIT",
    "driver": "{ODBC Driver 17 for SQL Server}",
    "trusted_connection": "yes",
}

LLM_CONFIG = {
    "api_key": _api_key,
    "api_version": _version or "2024-06-01",
    "azure_endpoint": _endpoint,
    "deployment_name": _deployment or "gpt-4o",
    "model_name": _deployment or "gpt-4o",
    "max_tokens": 16000,  # Increased from 3000 to prevent truncation (gpt-4o max is 16384)
    "temperature": 0.1,
}

PROMPT_VERSION = "v6.0-enterprise"

# Processing settings
BATCH_SIZE = 10                 # Default claim batch size
MAX_PAYLOAD_CHARS = 100000
ENABLE_VALIDATION = True
LOG_FILE = "llm_summariser_v4.log"
WINDOW_DAYS = 30                # Sliding window size
REBUILD_INTERVAL_DAYS = 30      # Force rebuild after this many days (Section 7)
STUCK_TIMEOUT_MINUTES = 15      # Minutes before IN_PROGRESS is considered stuck

# Retry settings (Section 4)
MAX_API_RETRIES = 5
BASE_DELAY = 1.0                # seconds
MAX_DELAY = 60.0                # seconds
MAX_DB_RETRIES = 3              # DB retries for claim/complete/fail

# Token cost rates (GPT-4o, USD per 1M tokens)
COST_PER_1M_INPUT = 2.50
COST_PER_1M_OUTPUT = 10.00


# ============================================================
# LOGGING
# ============================================================

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
# SYSTEM PROMPT (unchanged — full summarisation)
# ============================================================

SYSTEM_PROMPT = """You are a Customer Experience Analyst at Virgin Media Ireland.
You will receive a JSON payload containing a customer's complete interaction history over the last 30 days.

THE PAYLOAD CONTAINS 4 DATA SOURCES - YOU MUST USE ALL OF THEM:

1. "interactions" - Call centre interaction records
   - date, interaction_type, interaction_id, agent_wrapup_comment
   - The agent_wrapup_comment is the agent's own notes about what happened on the call
   - Use these to understand the volume and nature of customer contacts

2. "call_recordings" - Call recordings WITH pre-analyzed AI intelligence
   - Each recording includes PRE-ANALYZED fields you MUST use:
     - "call_summary": AI-generated summary of the call
     - "call_issues": Pre-classified intent with confidence and resolution status
       - "intent": The classified reason (e.g. "Account closure or cancellation")
       - "intent_family": Category (e.g. "service_cancellation_requests")
       - "resolution_status": "RESOLVED" or "UNRESOLVED"
       - "confidence_band": "HIGH", "MEDIUM", or "LOW"
     - "call_root_causes": Pre-identified root causes with confidence
     - "customer_quotes": EXACT verified quotes from the customer
   - NOT all calls will have pre-analyzed data. If call_summary is null, use interaction wrap-up comments instead.

3. "pega_cases" - Pega trouble tickets and work orders
   - case_id, status, case_type, case_sub_type, created_date, resolved_date, agent, closure_reason
   - Use these for open/resolved case tracking and issue identification

4. "servicenow_cases" - ServiceNow incidents
   - incident_number, status, title, summary, created_date, resolved_date
   - Use these for network/service incidents affecting the customer

5. "customer_profile" - Customer value & product holdings (may be null if unavailable)
   - Fetched from Revenue_Cache (nightly refresh from CERILLION/ITDEV)
   - "customer_type": 'Mobile Only', 'Mobile + Fixed', or 'Fixed Only'
   - "product_list": services held (e.g. 'Mobile, Fixed')
   - "service_status": e.g. 'Mobile: Active, Fixed: Active'
   - "monthly_revenue_total" / "annual_revenue_total": combined revenue
   - "revenue_segment": 'High Value' (€100+/mo), 'Medium Value' (€50-99), 'Low Value' (<€50)
   - "contract_end_fixed": fixed service contract end date (extrapolated if active)
   - "revenue_cached_at": when revenue was last refreshed
   - USE THIS to assess the business impact of unresolved issues
   - If customer_type is 'Mobile + Fixed' and the issue is with mobile, HIGHLIGHT that fixed services are also at risk of churn

CRITICAL RULES:
1. USE ALL 4 DATA SOURCES to build the complete customer story.
2. For call details: USE pre-analyzed call_summary, call_issues, and customer_quotes as PRIMARY source.
3. For calls WITHOUT pre-analyzed data: USE the interaction agent_wrapup_comment instead.
4. Use "customer_quotes" for direct quotes - these are VERIFIED extracts from actual calls.
5. Use "call_issues.intent" for why the customer called - do NOT reclassify or reinterpret.
6. Use "call_issues.resolution_status" for whether the call issue was resolved - do NOT guess.
7. For case resolution: USE pega_cases.status and servicenow_cases.status - do NOT guess.
8. ONLY state facts directly present in the data. Never infer, guess, or assume.
9. ALWAYS reference specific dates, case IDs, incident numbers, and agent names from the data.
10. If data is missing, use "No data available" - NEVER fill gaps with assumptions.
11. Do NOT use "it appears", "likely", "probably", "it seems". State facts or say unknown.
12. Every claim must be traceable to a specific record in the data.
13. Do NOT calculate escalation_risk — leave it as "Pending" (Python calculates this post-LLM).

Respond with ONLY valid JSON in this EXACT structure (no markdown, no backticks, no preamble):

{
    "customer_id": "from the data",
    "total_contacts_30d": number,
    "sentiment": "Positive|Negative|Neutral|Mixed",
    "sentiment_reason": "1 sentence based on customer_quotes, wrap-up comments, and case statuses from the data",
    "escalation_risk": "Pending",
    "escalation_risk_reason": "Pending — calculated by system",
    "is_repeat_caller": true|false,
    "repeat_caller_detail": "what issue they keep calling about using call_issues.intent, or N/A",
    "resolution_status": "Fully Resolved|Partially Resolved|Unresolved|Ongoing - based on call_issues.resolution_status AND pega/SNOW case statuses combined",
    "account_value": {
        "monthly_revenue": "from customer_profile.monthly_revenue_total or 'Unknown'",
        "annual_revenue": "from customer_profile.annual_revenue_total or 'Unknown'",
        "revenue_segment": "from customer_profile.revenue_segment or 'Unknown'",
        "products_held": "from customer_profile.product_list or 'Unknown'",
        "customer_type": "from customer_profile.customer_type or 'Unknown'",
        "service_status": "from customer_profile.service_status or 'Unknown'",
        "contract_end_fixed": "from customer_profile.contract_end_fixed or null",
        "tenure_months": "from customer_profile.tenure_months. Integer or null if unknown.",
        "cross_product_risk": "true if customer_type is 'Mobile + Fixed' and current issue could cause full churn, false otherwise",
        "revenue_at_risk": "annual_revenue_total if cross_product_risk is true, else annual_revenue for the affected service only. Use 'Unknown' if no revenue data."
    },
    "contact_timeline": [
        {
            "date": "YYYY-MM-DD HH:MM",
            "type": "Call|Interaction|PegaCase|ServiceNow",
            "agent": "agent name or Unknown",
            "call_intent": "from call_issues.intent, or interaction_type, or case_type - depending on source",
            "call_resolution": "from call_issues.resolution_status, or case status, or N/A",
            "summary": "use call_summary, or agent_wrapup_comment, or case title/summary - depending on source",
            "source": "interaction_id, recording filename, case_id, or incident_number"
        }
    ],
    "customer_voice": [
        {
            "date": "YYYY-MM-DD",
            "quote": "EXACT quote from customer_quotes field",
            "context": "which call this was from (agent name, intent)",
            "source": "recording filename"
        }
    ],
    "call_intents_summary": [
        {
            "intent": "from call_issues.intent",
            "intent_family": "from call_issues.intent_family",
            "occurrences": number,
            "resolution_status": "RESOLVED|UNRESOLVED",
            "confidence": "HIGH|MEDIUM|LOW"
        }
    ],
    "open_cases": [
        {
            "case_id": "case ID",
            "system": "Pega|ServiceNow",
            "type": "case type",
            "sub_type": "sub type or N/A",
            "created_date": "YYYY-MM-DD",
            "status": "current status",
            "assigned_to": "agent or workbasket"
        }
    ],
    "resolved_cases": [
        {
            "case_id": "case ID",
            "system": "Pega|ServiceNow",
            "type": "case type",
            "created_date": "YYYY-MM-DD",
            "resolved_date": "YYYY-MM-DD",
            "closure_reason": "from data or N/A"
        }
    ],
    "key_issues": [
        {
            "issue": "from call_issues.intent, pega case_type, SNOW title, or wrap-up comments",
            "related_case_ids": ["pega case_id and/or SNOW incident_number linked to this issue"],
            "status": "Open|Resolved",
            "root_cause": "from call_root_causes if available, or N/A"
        }
    ],
    "agent_briefing": "2-3 sentences MAX. Start with customer value context if available (e.g. 'High Value customer paying €X/month with Mobile + Broadband + TV'). Then the most critical issue. If cross_product_risk is true, warn that the entire account is at risk. Be direct and actionable.",
    "recommended_actions": [
        "specific action referencing case IDs or intents"
    ],
    "interactions_summary": "2-3 sentences summarising the interaction pattern. How many calls, over what period, what were the main reasons (from interaction_type and agent_wrapup_comment). Mention specific interaction IDs. If no interactions, say 'No interactions recorded.'",
    "pega_cases_summary": "2-3 sentences summarising all Pega cases. How many open vs resolved, what types, key closure reasons. Reference specific case IDs. If no Pega cases, say 'No Pega cases recorded.'",
    "servicenow_summary": "2-3 sentences summarising all ServiceNow incidents. How many open vs resolved, what types, key outcomes. Reference specific incident numbers. If no ServiceNow cases, say 'No ServiceNow incidents recorded.'"
}

If there are no items for an array field, return an empty array [].

CRITICAL REQUIREMENTS:
- All revenue fields: Use €X.XX format (e.g., "€150.00", "€1,800.00")
- tenure_months: integer (e.g., 36) or null. Do NOT use "Unknown".
- cross_product_risk: boolean true|false only
- escalation_risk: ALWAYS set to "Pending" (system calculates)
- ACCURACY OVER COMPLETENESS: Say less and be correct rather than more and be wrong."""


# ============================================================
# INCREMENTAL PROMPT (Scenario 2)
# ============================================================

INCREMENTAL_PROMPT = """You are a Customer Experience Analyst at Virgin Media Ireland.

You will receive TWO inputs:
1. "existing_summary" - The customer's CURRENT summary JSON (generated from previous events)
2. "new_events" - NEW events that have occurred SINCE the last summary

YOUR TASK: Merge the new events into the existing summary to produce an UPDATED summary.

MERGE RULES:
1. KEEP all historical context from existing_summary (don't drop resolved cases, past timeline entries, etc.)
2. ADD new events to contact_timeline (append, keep chronological order)
3. UPDATE open_cases: if a previously-open case now appears resolved in new_events, move it to resolved_cases
4. UPDATE sentiment based on the COMBINED picture (old + new)
5. UPDATE total_contacts_30d to reflect the new total
6. UPDATE is_repeat_caller based on the combined contact pattern
7. ADD any new customer_quotes from new call recordings
8. ADD any new call_intents from new recordings
9. UPDATE key_issues: add new issues, update status of existing ones
10. REWRITE agent_briefing to reflect the CURRENT state (most important issues NOW)
11. REWRITE recommended_actions based on current state
12. UPDATE resolution_status based on combined case/call statuses
13. UPDATE interactions_summary, pega_cases_summary, servicenow_summary with new data
14. PRESERVE account_value from existing summary (revenue doesn't change between runs)
15. Set escalation_risk to "Pending" (system calculates post-merge)

CRITICAL: The output must use the EXACT SAME JSON schema as the existing summary.
CRITICAL: Do NOT fabricate data. Only add facts from new_events.
CRITICAL: If new_events is empty or has no substance, return the existing summary unchanged.

Respond with ONLY valid JSON (no markdown, no backticks, no preamble)."""


# ============================================================
# VALIDATION PROMPT (unchanged)
# ============================================================

VALIDATION_PROMPT = """You are a QA checker. You will receive source data and a JSON summary generated from it.

Check:
- Are all dates in the summary actually in the source data?
- Are all case IDs in the summary actually in the source data?
- Are all agent names in the summary actually in the source data?
- Are revenue figures and product holdings accurate vs customer_profile in source data?
- Does the summary make claims not supported by the data?
- Are any details fabricated?

Respond with ONLY:
- "PASS" if factually accurate
- "FAIL: [list specific inaccuracies]" if problems found

Be strict. Any detail not traceable to source data is a failure."""


# ============================================================
# TEMPLATE FORMATTER (unchanged except escalation_risk_score)
# ============================================================

def format_summary_template(parsed_json, customer_id):
    d = parsed_json
    lines = []
    lines.append("=" * 60)
    lines.append("CUSTOMER 360 SUMMARY")
    lines.append("=" * 60)
    lines.append("")

    lines.append("--- CUSTOMER SNAPSHOT ---")
    lines.append(f"Customer ID          : {d.get('customer_id', customer_id)}")
    lines.append(f"Total Contacts (30d) : {d.get('total_contacts_30d', 'N/A')}")
    lines.append(f"Sentiment            : {d.get('sentiment', 'Unknown')}")
    lines.append(f"Sentiment Reason     : {d.get('sentiment_reason', 'N/A')}")
    lines.append(f"Repeat Caller        : {'Yes' if d.get('is_repeat_caller') else 'No'}")
    if d.get('is_repeat_caller') and d.get('repeat_caller_detail', 'N/A') != 'N/A':
        lines.append(f"Repeat Detail        : {d.get('repeat_caller_detail')}")
    lines.append(f"Resolution Status    : {d.get('resolution_status', 'Unknown')}")
    lines.append(f"Escalation Risk      : {d.get('escalation_risk', 'Unknown')}")
    if d.get('escalation_risk_score') is not None:
        lines.append(f"Escalation Score     : {d.get('escalation_risk_score')}")
    lines.append(f"Escalation Reason    : {d.get('escalation_risk_reason', 'N/A')}")
    lines.append("")

    av = d.get('account_value', {})
    if av and av.get('monthly_revenue') != 'Unknown':
        lines.append("--- ACCOUNT VALUE ---")
        lines.append(f"Monthly Revenue      : {av.get('monthly_revenue', 'Unknown')}")
        lines.append(f"Annual Revenue       : {av.get('annual_revenue', 'Unknown')}")
        lines.append(f"Revenue Segment      : {av.get('revenue_segment', 'Unknown')}")
        lines.append(f"Products Held        : {av.get('products_held', 'Unknown')}")
        lines.append(f"Customer Type        : {av.get('customer_type', 'Unknown')}")
        lines.append(f"Service Status       : {av.get('service_status', 'Unknown')}")
        tenure = av.get('tenure_months')
        if tenure is not None:
            lines.append(f"Tenure               : {tenure} months")
        if av.get('contract_end_fixed'):
            lines.append(f"Contract End (Fixed) : {av.get('contract_end_fixed')}")
        if av.get('cross_product_risk'):
            lines.append(f"*** CROSS-PRODUCT RISK: Full account at risk ({av.get('revenue_at_risk', 'Unknown')}/year) ***")
        lines.append("")

    lines.append("--- WHAT THE AGENT NEEDS TO KNOW ---")
    lines.append(d.get('agent_briefing', 'No briefing available.'))
    lines.append("")

    lines.append("--- CALL INTENTS (Pre-Analyzed) ---")
    intents = d.get('call_intents_summary', [])
    if intents:
        for intent in intents:
            # Handle both dict and string formats
            if isinstance(intent, dict):
                lines.append(f"  Intent       : {intent.get('intent', 'N/A')}")
                lines.append(f"  Family       : {intent.get('intent_family', 'N/A')}")
                lines.append(f"  Occurrences  : {intent.get('occurrences', 'N/A')}")
                lines.append(f"  Resolution   : {intent.get('resolution_status', 'N/A')}")
                lines.append(f"  Confidence   : {intent.get('confidence', 'N/A')}")
            elif isinstance(intent, str):
                lines.append(f"  Intent       : {intent}")
                lines.append(f"  Family       : N/A")
                lines.append(f"  Occurrences  : N/A")
                lines.append(f"  Resolution   : N/A")
                lines.append(f"  Confidence   : N/A")
            lines.append("")
    else:
        lines.append("  No call intent data available.")
    lines.append("")

    lines.append("--- CUSTOMER VOICE (Verified Quotes) ---")
    quotes = d.get('customer_voice', [])
    if quotes:
        for q in quotes:
            lines.append(f"  [{q.get('date', 'Unknown')}] \"{q.get('quote', 'N/A')}\"")
            lines.append(f"    Context: {q.get('context', 'N/A')}")
            lines.append(f"    Source:  {q.get('source', 'N/A')}")
            lines.append("")
    else:
        lines.append("  No customer quotes available for this period.")
    lines.append("")

    lines.append("--- CONTACT TIMELINE ---")
    timeline = d.get('contact_timeline', [])
    if timeline:
        for idx, event in enumerate(timeline, 1):
            lines.append(f"  {idx}. [{event.get('date', 'Unknown date')}] "
                         f"{event.get('type', 'Unknown')} - "
                         f"Agent: {event.get('agent', 'Unknown')}")
            lines.append(f"     Intent:     {event.get('call_intent', 'N/A')}")
            lines.append(f"     Resolution: {event.get('call_resolution', 'N/A')}")
            lines.append(f"     Summary:    {event.get('summary', 'No details')}")
            lines.append(f"     Source:     {event.get('source', 'N/A')}")
            lines.append("")
    else:
        lines.append("  No contact history available.")
    lines.append("")

    lines.append("--- OPEN CASES ---")
    for c in d.get('open_cases', []):
        lines.append(f"  Case: {c.get('case_id', 'N/A')} ({c.get('system', 'Unknown')})")
        lines.append(f"    Type       : {c.get('type', 'N/A')} / {c.get('sub_type', 'N/A')}")
        lines.append(f"    Created    : {c.get('created_date', 'N/A')}")
        lines.append(f"    Status     : {c.get('status', 'N/A')}")
        lines.append(f"    Assigned   : {c.get('assigned_to', 'N/A')}")
        lines.append("")
    if not d.get('open_cases'):
        lines.append("  No open cases.")
    lines.append("")

    lines.append("--- RESOLVED CASES ---")
    for c in d.get('resolved_cases', []):
        lines.append(f"  Case: {c.get('case_id', 'N/A')} ({c.get('system', 'Unknown')})")
        lines.append(f"    Type       : {c.get('type', 'N/A')}")
        lines.append(f"    Created    : {c.get('created_date', 'N/A')}")
        lines.append(f"    Resolved   : {c.get('resolved_date', 'N/A')}")
        lines.append(f"    Closure    : {c.get('closure_reason', 'N/A')}")
        lines.append("")
    if not d.get('resolved_cases'):
        lines.append("  No resolved cases in this period.")
    lines.append("")

    lines.append("--- KEY ISSUES ---")
    issues = d.get('key_issues', [])
    if issues:
        for idx, issue in enumerate(issues, 1):
            case_refs = ", ".join(issue.get('related_case_ids', []))
            lines.append(f"  {idx}. {issue.get('issue', 'N/A')}")
            lines.append(f"     Cases:      {case_refs if case_refs else 'N/A'}")
            lines.append(f"     Status:     {issue.get('status', 'Unknown')}")
            lines.append(f"     Root Cause: {issue.get('root_cause', 'N/A')}")
    else:
        lines.append("  No specific issues identified.")
    lines.append("")

    lines.append("--- RECOMMENDED ACTIONS ---")
    actions = d.get('recommended_actions', [])
    if actions:
        for idx, action in enumerate(actions, 1):
            lines.append(f"  {idx}. {action}")
    else:
        lines.append("  No specific actions recommended.")
    lines.append("")

    lines.append("--- INTERACTIONS SUMMARY ---")
    lines.append(d.get('interactions_summary', 'No interactions data available.'))
    lines.append("")
    lines.append("--- PEGA CASES SUMMARY ---")
    lines.append(d.get('pega_cases_summary', 'No Pega cases data available.'))
    lines.append("")
    lines.append("--- SERVICENOW INCIDENTS SUMMARY ---")
    lines.append(d.get('servicenow_summary', 'No ServiceNow incidents data available.'))
    lines.append("")

    lines.append("=" * 60)
    lines.append(f"Generated: {{timestamp}} | Model: {LLM_CONFIG['model_name']} | Prompt: {PROMPT_VERSION}")
    lines.append("=" * 60)
    return "\n".join(lines)


# ============================================================
# THIN DATA DETECTION (unchanged)
# ============================================================

MIN_DATA_FOR_LLM = {
    "min_interactions": 2,
    "min_recordings_with_analysis": 1,
    "min_cases": 1,
    "min_wrapup_comments": 1,
}


def has_enough_data_for_llm(payload):
    ds = payload["data_summary"]
    interactions = payload.get("interactions", [])
    meaningful_wrapups = sum(
        1 for i in interactions
        if i.get("agent_wrapup_comment")
        and str(i["agent_wrapup_comment"]).strip().upper() not in ("N/A", "", "NONE", "NULL")
    )
    has_recordings = ds.get("recordings_with_analysis", 0) >= MIN_DATA_FOR_LLM["min_recordings_with_analysis"]
    has_cases = (ds.get("total_pega_cases", 0) + ds.get("total_servicenow_cases", 0)) >= MIN_DATA_FOR_LLM["min_cases"]
    has_wrapups = meaningful_wrapups >= MIN_DATA_FOR_LLM["min_wrapup_comments"]
    has_interactions = ds.get("total_interactions", 0) >= MIN_DATA_FOR_LLM["min_interactions"]
    has_substance = has_recordings or has_cases or has_wrapups

    if has_interactions and has_substance:
        return True, "Sufficient data"
    if not has_interactions and not has_substance:
        return False, "No meaningful data"
    if ds.get("total_interactions", 0) <= 1 and not has_substance:
        return False, f"Only {ds.get('total_interactions', 0)} interaction(s) with no recordings, cases, or wrap-up comments"
    if ds.get("total_interactions", 0) >= 3 and not has_substance:
        return False, f"{ds.get('total_interactions', 0)} interactions but no meaningful comments, recordings, or cases"
    return True, "Sufficient data"


def build_thin_data_summary(payload, customer_id):
    ds = payload["data_summary"]
    interactions = payload.get("interactions", [])
    pega = payload.get("pega_cases", [])
    snow = payload.get("servicenow_cases", [])

    timeline = []
    for inter in interactions:
        timeline.append({
            "date": inter.get("date", "Unknown"), "type": "Interaction", "agent": "Unknown",
            "call_intent": inter.get("interaction_type", "N/A"), "call_resolution": "N/A",
            "summary": inter.get("agent_wrapup_comment") or "No details recorded",
            "source": inter.get("interaction_id", "N/A")
        })

    total = ds.get("total_interactions", 0)
    dates = [i.get("date", "") for i in interactions if i.get("date")]
    date_range = ""
    if dates:
        date_range = f" between {min(dates)[:10]} and {max(dates)[:10]}" if len(dates) > 1 else f" on {dates[0][:10]}"

    comments = [
        i.get("agent_wrapup_comment") for i in interactions
        if i.get("agent_wrapup_comment")
        and str(i["agent_wrapup_comment"]).strip().upper() not in ("N/A", "", "NONE", "NULL")
    ]

    parsed_json = {
        "customer_id": customer_id, "total_contacts_30d": total,
        "sentiment": "Unknown", "sentiment_reason": "Insufficient data to determine sentiment.",
        "escalation_risk": "Pending",
        "escalation_risk_reason": "Pending — calculated by system",
        "is_repeat_caller": total > 2, "repeat_caller_detail": "N/A", "resolution_status": "Unknown",
        "contact_timeline": timeline, "customer_voice": [], "call_intents_summary": [],
        "open_cases": [
            {"case_id": c.get("case_id", "N/A"), "system": "Pega", "type": c.get("case_type", "N/A"),
             "sub_type": c.get("case_sub_type", "N/A"), "created_date": str(c.get("created_date", "N/A"))[:10],
             "status": c.get("status", "N/A"), "assigned_to": c.get("agent") or c.get("workbasket", "N/A")}
            for c in pega if c.get("status") and "resolved" not in str(c.get("status", "")).lower() and "closed" not in str(c.get("status", "")).lower()
        ] + [
            {"case_id": c.get("incident_number", "N/A"), "system": "ServiceNow", "type": c.get("title", "N/A"),
             "sub_type": "N/A", "created_date": str(c.get("created_date", "N/A"))[:10],
             "status": c.get("status", "N/A"), "assigned_to": c.get("agent_working_on", "N/A")}
            for c in snow if c.get("status") and "resolved" not in str(c.get("status", "")).lower() and "closed" not in str(c.get("status", "")).lower()
        ],
        "resolved_cases": [], "key_issues": [],
        "agent_briefing": f"Customer had {total} interaction(s){date_range}. "
                          + (f"Agent noted: \"{comments[0][:200]}\"" if comments else "No agent comments were recorded.")
                          + " Limited data available for this customer.",
        "recommended_actions": [],
        "interactions_summary": f"{total} phone interaction(s) recorded{date_range}. "
                                + (f"Agent wrap-up comments available for {len(comments)} interaction(s)." if comments else "No agent wrap-up comments recorded."),
        "pega_cases_summary": f"{len(pega)} Pega case(s) recorded." if pega else "No Pega cases recorded.",
        "servicenow_summary": f"{len(snow)} ServiceNow incident(s) recorded." if snow else "No ServiceNow incidents recorded."
    }

    profile = payload.get("customer_profile")
    devices = payload.get("devices", [])
    if profile:
        parsed_json["account_value"] = {
            "monthly_revenue": f"€{profile['monthly_revenue_total']:.2f}" if profile.get('monthly_revenue_total') else "Unknown",
            "annual_revenue": f"€{profile['annual_revenue_total']:.2f}" if profile.get('annual_revenue_total') else "Unknown",
            "revenue_segment": profile.get('revenue_segment', 'Unknown'),
            "products_held": profile.get('product_list', 'Unknown'),
            "customer_type": profile.get('customer_type', 'Unknown'),
            "service_status": profile.get('service_status', 'Unknown'),
            "contract_end_fixed": profile.get('contract_end_fixed'), "tenure_months": profile.get('tenure_months'),
            "cross_product_risk": False, "revenue_at_risk": "Unknown",
            # New fields from Revenue_Cache
            "plan_count": profile.get('plan_count'),
            "account_category": profile.get('account_category'),
            "device_count": profile.get('device_count'),
            "device_financing_revenue": f"€{profile['device_financing_revenue']:.2f}" if profile.get('device_financing_revenue') else None,
        }
        if devices:
            parsed_json["account_value"]["devices"] = devices
            # Calculate device portfolio summary
            active_contracts = sum(1 for d in devices if d.get('is_contract_active'))
            expired_contracts = len(devices) - active_contracts
            total_mic = sum(d.get('mic_monthly') or 0 for d in devices)
            parsed_json["account_value"]["device_portfolio"] = {
                'total_devices': len(devices),
                'active_contracts': active_contracts,
                'expired_contracts': expired_contracts,
                'total_mic_monthly': total_mic,
                'device_summary': f"{active_contracts} active, {expired_contracts} expired, €{total_mic:.0f}/month MIC"
            }
        if profile.get('monthly_revenue_total'):
            device_info = ""
            if devices:
                active_contracts = sum(1 for d in devices if d.get('is_contract_active'))
                device_info = f", {active_contracts} active device(s)"
            parsed_json["agent_briefing"] = (
                f"{profile.get('revenue_segment', '')} customer ({profile['monthly_revenue_total']:.0f}/month, "
                f"{profile.get('product_list', 'Unknown')}{device_info}). " + parsed_json["agent_briefing"]
            )
    else:
        parsed_json["account_value"] = {
            "monthly_revenue": "Unknown", "annual_revenue": "Unknown", "revenue_segment": "Unknown",
            "products_held": "Unknown", "customer_type": "Unknown", "service_status": "Unknown",
            "contract_end_fixed": None, "tenure_months": None, "cross_product_risk": False, "revenue_at_risk": "Unknown",
            # New fields with default values
            "plan_count": None, "account_category": None, "device_count": None, "device_financing_revenue": None
        }
    return parsed_json


# ============================================================
# DATABASE CONNECTIONS
# ============================================================

def get_connection(config=None):
    cfg = config or DB_CONFIG
    conn_str = f"DRIVER={cfg['driver']};SERVER={cfg['server']};DATABASE={cfg['database']};"
    if cfg.get("trusted_connection") == "yes":
        conn_str += "Trusted_Connection=yes;"
    else:
        conn_str += f"UID={cfg['uid']};PWD={cfg['pwd']};"
    return pyodbc.connect(conn_str)


def get_llm_client():
    return AzureOpenAI(
        api_key=LLM_CONFIG["api_key"],
        api_version=LLM_CONFIG["api_version"],
        azure_endpoint=LLM_CONFIG["azure_endpoint"],
    )


# ============================================================
# REVENUE FROM Revenue_Cache (Section 3)
# Replaces live IEROXAPP2 queries with local cache read.
# ============================================================

def fetch_customer_profile(conn, customer_id):
    """Read revenue from Revenue_Cache (local, fast, crash-independent)."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT [customer_type], [product_list], [service_status],
               [monthly_revenue_total], [annual_revenue_total],
               [revenue_segment], [contract_end_fixed], [tenure_months],
               [has_mobile], [has_fixed], [cached_at],
               [monthly_revenue_mobile], [monthly_revenue_fixed],
               [mobile_active], [fixed_active],
               [mobile_account], [fixed_account],
               [plan_count], [account_category], [device_count], [device_financing_revenue]
        FROM [dbo].[Revenue_Cache]
        WHERE [customer_id] = ?
    """, customer_id)
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    return {
        'customer_type': row[0], 'product_list': row[1], 'service_status': row[2],
        'monthly_revenue_total': float(row[3]) if row[3] else None,
        'annual_revenue_total': float(row[4]) if row[4] else None,
        'revenue_segment': row[5],
        'contract_end_fixed': str(row[6]) if row[6] else None,
        'tenure_months': row[7],
        'has_mobile': bool(row[8]), 'has_fixed': bool(row[9]),
        'revenue_cached_at': str(row[10]) if row[10] else None,
        'monthly_revenue_mobile': float(row[11]) if row[11] else None,
        'monthly_revenue_fixed': float(row[12]) if row[12] else None,
        'mobile_active': bool(row[13]), 'fixed_active': bool(row[14]),
        'mobile_account': row[15], 'fixed_account': row[16],
        'plan_count': row[17], 'account_category': row[18],
        'device_count': row[19], 'device_financing_revenue': float(row[20]) if row[20] else None,
    }


def fetch_customer_devices(conn, customer_id):
    """Read device assets from Customer_Device_Assets."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT [device_id], [device_brand], [device_model], [device_colour],
               [device_memory], [device_value], [down_payment], [installment_count],
               [installment_amount], [installments_remaining], [contract_start_date],
               [contract_end_date], [contract_status], [device_status],
               [package_code], [package_name], [imei], [sim_serial_number],
               [mic_monthly], [is_contract_active]
        FROM [dbo].[Customer_Device_Assets]
        WHERE [customer_id] = ?
        ORDER BY [contract_end_date] DESC
    """, customer_id)
    devices = []
    for row in cursor.fetchall():
        devices.append({
            'device_id': row[0],
            'device_brand': row[1],
            'device_model': row[2],
            'device_colour': row[3],
            'device_memory': row[4],
            'device_value': float(row[5]) if row[5] else None,
            'down_payment': float(row[6]) if row[6] else None,
            'installment_count': row[7],
            'installment_amount': float(row[8]) if row[8] else None,
            'installments_remaining': row[9],
            'contract_start_date': str(row[10]) if row[10] else None,
            'contract_end_date': str(row[11]) if row[11] else None,
            'contract_status': row[12],
            'device_status': row[13],
            'package_code': row[14],
            'package_name': row[15],
            'imei': row[16],
            'sim_serial_number': row[17],
            'mic_monthly': float(row[18]) if row[18] else 0,
            'is_contract_active': bool(row[19]),
        })
    cursor.close()
    return devices


# ============================================================
# DETERMINE SCENARIO (Sections 1 + 7)
# Deterministic window from run_date + periodic rebuild.
# ============================================================

def determine_scenario(conn, customer_id, window_start, run_date):
    """
    Returns: (scenario, watermark, existing_summary_json)
      scenario: 'FULL' | 'INCREMENTAL' | 'REBUILD'
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT [last_processed_event_ts], [summary_json],
               [rolling_summary_text], [last_full_build_date]
        FROM [dbo].[LLM_Customer_Summary]
        WHERE [customer_id] = ?
    """, customer_id)
    row = cursor.fetchone()
    cursor.close()

    # Scenario 1: No row or empty content → FULL
    if row is None:
        return 'FULL', None, None
    watermark, existing_json, existing_text, last_full = row[0], row[1], row[2], row[3]
    if existing_text is None or existing_json is None:
        return 'FULL', None, None

    # Scenario 3a: No last_full_build_date recorded → REBUILD
    if last_full is None:
        return 'REBUILD', watermark, existing_json

    # Scenario 3b: Periodic rebuild governance (>30 days since last full)
    days_since_full = (run_date - last_full).days
    if days_since_full > REBUILD_INTERVAL_DAYS:
        logger.info(f"  Forced REBUILD: {days_since_full} days since last full build")
        return 'REBUILD', watermark, existing_json

    # Scenario 3c: Stale watermark
    if watermark is None:
        return 'REBUILD', None, existing_json

    # FIX: Safely extract date from watermark (handles both datetime and date)
    if isinstance(watermark, datetime):
        watermark_date = watermark.date()
    else:
        watermark_date = watermark

    if watermark_date < window_start:
        return 'REBUILD', watermark, existing_json

    # Scenario 2: Watermark within window → INCREMENTAL
    return 'INCREMENTAL', watermark, existing_json


# ============================================================
# DETERMINISTIC ESCALATION SCORING (Section 6)
# Python controls risk score, not LLM.
# ============================================================

def calculate_escalation_risk(parsed_json, customer_profile):
    """
    Weighted composite score:
      Repeat contacts     25%  (0=1 contact, 1=2-3, 2=4+)
      Unresolved issues   35%  (0=fully resolved, 1=partial, 2=unresolved)
      Sentiment           25%  (0=positive, 1=neutral/mixed, 2=negative)
      Revenue impact      15%  (0=low, 1=medium, 2=high)
    """
    contacts = parsed_json.get('total_contacts_30d', 0)
    if isinstance(contacts, str):
        try:
            contacts = int(contacts)
        except (ValueError, TypeError):
            contacts = 0

    resolution = parsed_json.get('resolution_status', 'Unknown')
    sentiment = parsed_json.get('sentiment', 'Unknown')
    revenue_segment = (customer_profile or {}).get('revenue_segment', 'Unknown')

    # Contact frequency
    if contacts >= 4:    contact_score = 2
    elif contacts >= 2:  contact_score = 1
    else:                contact_score = 0

    # Resolution
    res_lower = str(resolution).lower()
    if 'unresolved' in res_lower or 'ongoing' in res_lower:
        resolution_score = 2
    elif 'partial' in res_lower:
        resolution_score = 1
    else:
        resolution_score = 0

    # Sentiment
    sent_lower = str(sentiment).lower()
    if sent_lower == 'negative':
        sentiment_score = 2
    elif sent_lower in ('mixed', 'neutral', 'unknown'):
        sentiment_score = 1
    else:
        sentiment_score = 0

    # Revenue
    if revenue_segment == 'High Value':     revenue_score = 2
    elif revenue_segment == 'Medium Value': revenue_score = 1
    else:                                   revenue_score = 0

    composite = (
        contact_score * 0.25 +
        resolution_score * 0.35 +
        sentiment_score * 0.25 +
        revenue_score * 0.15
    )

    if composite >= 1.5:   risk = 'High'
    elif composite >= 0.8: risk = 'Medium'
    else:                  risk = 'Low'

    reason = (
        f"Score {composite:.2f}: "
        f"contacts={contacts}({contact_score}), "
        f"resolution={resolution}({resolution_score}), "
        f"sentiment={sentiment}({sentiment_score}), "
        f"revenue={revenue_segment}({revenue_score})"
    )

    return risk, round(composite, 2), reason


# ============================================================
# BUILD CUSTOMER PAYLOAD
# since_timestamp: if set (Scenario 2), only events AFTER it.
# Revenue now from Revenue_Cache (same conn, no revenue_conn).
# ============================================================

def build_customer_payload(conn, customer_id, since_timestamp=None):
    cursor = conn.cursor()
    payload = {
        "customer_id": customer_id,
        "interactions": [], "call_recordings": [],
        "pega_cases": [], "servicenow_cases": []
    }

    if since_timestamp:
        delta_clause = "AND (e.[event_timestamp] > ? OR e.[updated_at] > ?)"
        delta_params = [customer_id, since_timestamp, since_timestamp]
    else:
        delta_clause = ""
        delta_params = [customer_id]

    # --- INTERACTIONS ---
    cursor.execute(f"""
        SELECT e.[event_timestamp], e.[event_type], e.[natural_key], e.[event_detail_json]
        FROM [dbo].[Customer360_Events] e
        WHERE e.[customer_id] = ? AND e.[source_system] = 'Interaction' AND e.[is_deleted] = 0
        {delta_clause}
        ORDER BY e.[event_timestamp] DESC
    """, *delta_params)
    for row in cursor.fetchall():
        detail = _parse_json(row[3])
        payload["interactions"].append({
            "date": str(row[0]) if row[0] else None,
            "cti_call_id": detail.get("cti_call_id"),
            "interaction_type": row[1], "interaction_id": row[2],
            "agent_wrapup_comment": detail.get("wrapup_comment")
        })

    # --- CALL RECORDINGS ---
    cursor.execute(f"""
        SELECT e.[event_timestamp], e.[natural_key], e.[event_type],
               e.[event_detail_json], e.[recording_match_type],
               t.[audio_filename], t.[audio_duration],
               t.[call_start], t.[call_end],
               t.[agent_first_name], t.[agent_last_name],
               t.[cti_ani], t.[call_direction],
               t.[transcript_json], t.[transcript_status],
               t.[call_summary], t.[call_segment], t.[call_product],
               t.[call_issues_json], t.[call_root_causes_json],
               t.[customer_quotes_json]
        FROM [dbo].[Customer360_Events] e
        LEFT JOIN [dbo].[CallTranscript] t ON e.[transcript_id] = t.[transcript_id]
        WHERE e.[customer_id] = ? AND e.[source_system] = 'CallRecording' AND e.[is_deleted] = 0
        {delta_clause}
        ORDER BY e.[event_timestamp] DESC
    """, *delta_params)
    for row in cursor.fetchall():
        recording = {
            "filename": row[5], "call_start": str(row[7]) if row[7] else None,
            "call_end": str(row[8]) if row[8] else None, "duration": row[6],
            "agent": f"{row[9] or ''} {row[10] or ''}".strip(),
            "caller_number": row[11], "call_direction": row[12], "match_type": row[4],
            "call_summary": None, "call_segment": None, "call_product": None,
            "call_issues": [], "call_root_causes": [], "customer_quotes": [],
        }
        if row[15]:
            recording["call_summary"] = row[15]
            recording["call_segment"] = row[16]
            recording["call_product"] = row[17]
            _parse_call_intelligence(recording, row[18], row[19], row[20])
        elif row[13] and row[14] == 'Loaded':
            _parse_transcript_json(recording, row[13])
        payload["call_recordings"].append(recording)

    # --- PEGA CASES ---
    cursor.execute(f"""
        SELECT e.[event_timestamp], e.[natural_key], e.[event_type], e.[event_sub_type],
               e.[event_status], e.[event_resolved_at], e.[event_detail_json]
        FROM [dbo].[Customer360_Events] e
        WHERE e.[customer_id] = ? AND e.[source_system] = 'PegaCase' AND e.[is_deleted] = 0
        {delta_clause}
        ORDER BY e.[event_timestamp] DESC
    """, *delta_params)
    for row in cursor.fetchall():
        detail = _parse_json(row[6])
        payload["pega_cases"].append({
            "case_id": row[1], "status": row[4], "workbasket": detail.get("workbasket_heading"),
            "case_type": row[2], "case_sub_type": row[3],
            "created_date": str(row[0]) if row[0] else None,
            "resolved_date": str(row[5]) if row[5] else None,
            "agent": detail.get("agent"), "closure_reason": detail.get("closure_reason"),
            "workorder_type": detail.get("workorder_type")
        })

    # --- SERVICENOW CASES ---
    cursor.execute(f"""
        SELECT e.[event_timestamp], e.[natural_key], e.[event_type], e.[event_sub_type],
               e.[event_status], e.[event_resolved_at], e.[event_detail_json]
        FROM [dbo].[Customer360_Events] e
        WHERE e.[customer_id] = ? AND e.[source_system] = 'SNOWCase' AND e.[is_deleted] = 0
        {delta_clause}
        ORDER BY e.[event_timestamp] DESC
    """, *delta_params)
    for row in cursor.fetchall():
        detail = _parse_json(row[6])
        payload["servicenow_cases"].append({
            "incident_number": row[1], "status": row[4],
            "case_id": detail.get("case_id"), "title": detail.get("case_title") or row[2],
            "summary": detail.get("summary"), "workbasket_subtype": detail.get("workbasket_subtype") or row[3],
            "created_date": str(row[0]) if row[0] else None,
            "resolved_date": str(row[5]) if row[5] else None,
            "created_by": detail.get("created_by"), "agent_working_on": detail.get("agent_working_on")
        })

    cursor.close()

    # Revenue from Revenue_Cache (same connection, no cross-server)
    payload["customer_profile"] = None
    try:
        payload["customer_profile"] = fetch_customer_profile(conn, customer_id)
        if payload["customer_profile"]:
            p = payload["customer_profile"]
            logger.info(f"  Revenue: €{p.get('monthly_revenue_total') or 0:.0f}/month "
                         f"({p.get('revenue_segment', '?')}, {p.get('customer_type', '?')}, "
                         f"cached: {p.get('revenue_cached_at', '?')})")
        else:
            logger.info(f"  Revenue: Not found in Revenue_Cache")
    except Exception as e:
        logger.warning(f"  Revenue cache read failed for {customer_id}: {e}")

    # Devices from Customer_Device_Assets
    payload["devices"] = None
    try:
        payload["devices"] = fetch_customer_devices(conn, customer_id)
        if payload["devices"]:
            logger.info(f"  Devices: {len(payload['devices'])} asset(s)")
        else:
            logger.info(f"  Devices: No assets found")
    except Exception as e:
        logger.warning(f"  Device assets read failed for {customer_id}: {e}")
        payload["devices"] = []

    payload["data_summary"] = {
        "total_interactions": len(payload["interactions"]),
        "total_call_recordings": len(payload["call_recordings"]),
        "recordings_with_analysis": sum(1 for r in payload["call_recordings"] if r["call_summary"]),
        "recordings_with_quotes": sum(1 for r in payload["call_recordings"] if r["customer_quotes"]),
        "total_pega_cases": len(payload["pega_cases"]),
        "total_servicenow_cases": len(payload["servicenow_cases"])
    }
    return payload


# --- Payload helpers (unchanged) ---

def _parse_json(raw):
    if not raw: return {}
    try: return json.loads(raw) if isinstance(raw, str) else raw
    except: return {}

def _parse_call_intelligence(recording, issues_json, rc_json, quotes_json):
    if issues_json:
        try:
            issues = json.loads(issues_json) if isinstance(issues_json, str) else issues_json
            for issue in (issues if isinstance(issues, list) else []):
                recording["call_issues"].append({
                    "intent": issue.get("reason_display") or issue.get("reason_key"),
                    "intent_key": issue.get("reason_key"), "intent_family": issue.get("reason_family"),
                    "description": issue.get("reason_free_text"),
                    "resolution_status": issue.get("resolution_status"),
                    "confidence": issue.get("score"), "confidence_band": issue.get("calibrated_band"),
                })
        except: pass
    if rc_json:
        try:
            rcs = json.loads(rc_json) if isinstance(rc_json, str) else rc_json
            for rc in (rcs if isinstance(rcs, list) else []):
                recording["call_root_causes"].append({"label": rc.get("label"), "confidence": rc.get("confidence")})
        except: pass
    if quotes_json:
        try:
            quotes = json.loads(quotes_json) if isinstance(quotes_json, str) else quotes_json
            for q in (quotes if isinstance(quotes, list) else []):
                recording["customer_quotes"].append(q.get("quote") if isinstance(q, dict) else q)
        except: pass

def _parse_transcript_json(recording, transcript_raw):
    try:
        transcript = json.loads(transcript_raw) if isinstance(transcript_raw, str) else transcript_raw
        recording["call_summary"] = transcript.get("summary")
        recording["call_segment"] = transcript.get("segment")
        recording["call_product"] = transcript.get("product")
        for issue in transcript.get("issues", []):
            recording["call_issues"].append({
                "intent": issue.get("reason_display") or issue.get("reason_key"),
                "intent_key": issue.get("reason_key"), "intent_family": issue.get("reason_family"),
                "description": issue.get("reason_free_text"),
                "resolution_status": issue.get("resolution_status"),
                "confidence": issue.get("score"), "confidence_band": issue.get("calibrated_band"),
            })
        for rc in transcript.get("root_causes", []):
            recording["call_root_causes"].append({"label": rc.get("label"), "confidence": rc.get("confidence")})
        for q in transcript.get("salient_quotes", []):
            recording["customer_quotes"].append(q.get("quote"))
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"  Could not parse transcript JSON: {e}")


# ============================================================
# GET LATEST EVENT TIMESTAMP
# ============================================================

def get_latest_event_ts(conn, customer_id):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX([event_timestamp]) FROM [dbo].[Customer360_Events] WHERE [customer_id] = ? AND [is_deleted] = 0", customer_id)
    result = cursor.fetchone()
    cursor.close()
    return result[0] if result and result[0] else datetime.now()


# ============================================================
# LLM CALLING WITH RETRY (Section 4)
# ============================================================

def _call_api_with_retry(client, messages, max_tokens, temperature):
    """Azure OpenAI call with exponential backoff + jitter."""
    for attempt in range(MAX_API_RETRIES):
        try:
            response = client.chat.completions.create(
                model=LLM_CONFIG.get("deployment_name", LLM_CONFIG["model_name"]),
                messages=messages,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            return response
        except Exception as e:
            status_code = getattr(e, 'status_code', None)
            # Non-retryable
            if status_code in (400, 401, 403, 404):
                raise
            # Exhausted
            if attempt == MAX_API_RETRIES - 1:
                raise
            delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
            jitter = random.uniform(0, delay * 0.25)
            total_delay = delay + jitter
            logger.warning(f"  API retry {attempt+1}/{MAX_API_RETRIES}: "
                           f"{status_code or 'error'} — waiting {total_delay:.1f}s")
            time.sleep(total_delay)
    raise RuntimeError("Exhausted all API retries")


def call_llm(client, payload_json):
    """Full summarisation (Scenario 1 and 3). Returns (text, usage, duration)."""
    if len(payload_json) > MAX_PAYLOAD_CHARS:
        logger.warning(f"Payload too large ({len(payload_json)} chars), truncating")
        payload_json = payload_json[:MAX_PAYLOAD_CHARS] + "\n... [TRUNCATED]"
    start_time = time.time()
    response = _call_api_with_retry(client, [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": (
            "Below is the COMPLETE customer data. Your response must ONLY contain "
            "facts from this data. Respond with ONLY valid JSON.\n\n"
            f"{payload_json}"
        )}
    ], LLM_CONFIG["max_tokens"], LLM_CONFIG["temperature"])
    duration = time.time() - start_time
    usage = _extract_usage(response)
    return _clean_llm_response(response.choices[0].message.content), usage, duration


def call_llm_incremental(client, existing_summary_json, delta_payload_json):
    """Incremental merge (Scenario 2). Returns (text, usage, duration)."""

    # FIX: Calculate size BEFORE building combined string
    existing_size = len(existing_summary_json)
    delta_size = len(delta_payload_json)
    overhead = len("=== EXISTING SUMMARY ===\n\n=== NEW EVENTS SINCE LAST SUMMARY ===\n")
    total_size = existing_size + delta_size + overhead

    if total_size > MAX_PAYLOAD_CHARS:
        # Truncate delta BEFORE building combined
        available = MAX_PAYLOAD_CHARS - existing_size - overhead - 200  # 200 buffer for truncation marker
        delta_payload_json = delta_payload_json[:available] + "\n... [TRUNCATED]"
        logger.warning(f"Delta payload truncated: {delta_size} → {len(delta_payload_json)} chars")

    combined = (
        f"=== EXISTING SUMMARY ===\n{existing_summary_json}\n\n"
        f"=== NEW EVENTS SINCE LAST SUMMARY ===\n{delta_payload_json}"
    )

    start_time = time.time()
    response = _call_api_with_retry(client, [
        {"role": "system", "content": INCREMENTAL_PROMPT},
        {"role": "user", "content": (
            "Merge the new events into the existing summary. "
            "Output the COMPLETE updated summary as valid JSON.\n\n"
            f"{combined}"
        )}
    ], LLM_CONFIG["max_tokens"], LLM_CONFIG["temperature"])
    duration = time.time() - start_time
    usage = _extract_usage(response)
    return _clean_llm_response(response.choices[0].message.content), usage, duration


def _extract_usage(response):
    """Extract token usage from API response (Section 5)."""
    usage = getattr(response, 'usage', None)
    if usage:
        return {
            'input_tokens': getattr(usage, 'prompt_tokens', 0),
            'output_tokens': getattr(usage, 'completion_tokens', 0),
            'total_tokens': getattr(usage, 'total_tokens', 0),
        }
    return {'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0}


def _clean_llm_response(text):
    text = text.strip()
    if text.startswith("```json"): text = text[7:]
    if text.startswith("```"): text = text[3:]
    if text.endswith("```"): text = text[:-3]
    return text.strip()


def parse_llm_json(response_text, customer_id):
    try:
        return json.loads(response_text), True
    except json.JSONDecodeError as e:
        logger.error(f"  Failed to parse LLM JSON: {e}")
        return {
            "customer_id": customer_id, "total_contacts_30d": "N/A",
            "sentiment": "Unknown", "sentiment_reason": "LLM response could not be parsed",
            "escalation_risk": "Pending", "escalation_risk_reason": "Pending",
            "is_repeat_caller": False, "repeat_caller_detail": "N/A", "resolution_status": "Unknown",
            "contact_timeline": [], "customer_voice": [], "call_intents_summary": [],
            "open_cases": [], "resolved_cases": [], "key_issues": [],
            "agent_briefing": f"[PARSE ERROR] Raw LLM output: {response_text[:500]}",
            "recommended_actions": [],
            "account_value": {"monthly_revenue": "Unknown", "annual_revenue": "Unknown",
                "revenue_segment": "Unknown", "products_held": "Unknown", "customer_type": "Unknown",
                "service_status": "Unknown", "contract_end_fixed": None, "tenure_months": None,
                "cross_product_risk": False, "revenue_at_risk": "Unknown"},
            "interactions_summary": "LLM response could not be parsed.",
            "pega_cases_summary": "LLM response could not be parsed.",
            "servicenow_summary": "LLM response could not be parsed."
        }, False


def validate_summary(client, source_json, llm_json_text):
    try:
        response = _call_api_with_retry(client, [
            {"role": "system", "content": VALIDATION_PROMPT},
            {"role": "user", "content": f"=== SOURCE DATA ===\n{source_json}\n\n=== GENERATED SUMMARY ===\n{llm_json_text}"}
        ], 500, 0.0)
        usage = _extract_usage(response)
        result = response.choices[0].message.content.strip()
        return result.upper().startswith("PASS"), result, usage
    except Exception as e:
        logger.error(f"  Validation call failed: {e}")
        return True, "Validation skipped due to error", {'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0}


def regenerate_summary(client, payload_json, validation_feedback):
    response = _call_api_with_retry(client, [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Below is the COMPLETE customer data. Respond with ONLY valid JSON.\n\n{payload_json}"},
        {"role": "assistant", "content": "Let me re-examine the data carefully."},
        {"role": "user", "content": (
            f"Your previous response had accuracy issues:\n{validation_feedback}\n\n"
            "Please regenerate, fixing these issues. ONLY include facts directly present in the data. Respond with ONLY valid JSON."
        )}
    ], LLM_CONFIG["max_tokens"], 0.1)
    usage = _extract_usage(response)
    return _clean_llm_response(response.choices[0].message.content), usage


# ============================================================
# STATE MANAGEMENT — claim, complete, fail (Section 2)
# Uses stored procedures for atomicity.
# ============================================================

def claim_batch(conn, worker_id, batch_size, max_retries=3):
    """Atomic claim via sp_ClaimSummaryBatch. Returns list of customer_ids."""
    cursor = conn.cursor()
    cursor.execute("EXEC [dbo].[sp_ClaimSummaryBatch] @WorkerId=?, @BatchSize=?, @MaxRetries=?",
                   worker_id, batch_size, max_retries)
    claimed = [row[0] for row in cursor.fetchall()]
    conn.commit()
    cursor.close()
    return claimed


def mark_completed(conn, customer_id):
    cursor = conn.cursor()
    cursor.execute("EXEC [dbo].[sp_MarkSummaryCompleted] @CustomerId=?", customer_id)
    cursor.close()


def mark_failed(conn, customer_id, error_message):
    cursor = conn.cursor()
    cursor.execute("EXEC [dbo].[sp_MarkSummaryFailed] @CustomerId=?, @ErrorMessage=?",
                   customer_id, error_message[:2000])
    cursor.close()


def seed_pending(conn):
    """Seed PENDING rows for new customers and reset COMPLETED→PENDING for those with new events."""
    cursor = conn.cursor()
    cursor.execute("EXEC [dbo].[sp_SeedPendingSummaries]")
    conn.commit()
    cursor.close()
    logger.info("Seeded pending customers from vw_CustomersPendingSummary")


# ============================================================
# TOKEN LOGGING (Section 5)
# ============================================================

def append_token_log(conn, customer_id, run_date, scenario, usage, duration_ms, validation_tokens=0):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO [dbo].[LLM_Token_Log]
        ([customer_id], [run_date], [scenario], [prompt_version], [model_name],
         [input_tokens], [output_tokens], [total_tokens], [duration_ms], [validation_tokens])
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, customer_id, run_date, scenario, PROMPT_VERSION, LLM_CONFIG["model_name"],
         usage['input_tokens'], usage['output_tokens'], usage['total_tokens'],
         int(duration_ms), validation_tokens)
    cursor.close()


# ============================================================
# RUN LOGGING (Section 8)
# ============================================================

def insert_run_log(conn, run_date, window_start, worker_id):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO [dbo].[LLM_Run_Log]
        ([run_date], [window_start], [worker_id], [status])
        OUTPUT INSERTED.run_id
        VALUES (?, ?, ?, 'RUNNING')
    """, run_date, window_start, worker_id)
    run_id = int(cursor.fetchone()[0])
    conn.commit()
    cursor.close()
    return run_id


def finalise_run_log(conn, run_id, counters):
    cursor = conn.cursor()
    est_cost = (
        counters.get('total_input_tokens', 0) * COST_PER_1M_INPUT / 1_000_000
        + counters.get('total_output_tokens', 0) * COST_PER_1M_OUTPUT / 1_000_000
    )
    cursor.execute("""
        UPDATE [dbo].[LLM_Run_Log]
        SET [completed_at] = GETDATE(), [status] = 'COMPLETED',
            [total_customers] = ?, [scenario_full] = ?, [scenario_incr] = ?,
            [scenario_rebuild] = ?, [scenario_thin] = ?, [skipped] = ?,
            [errors] = ?, [total_tokens] = ?, [estimated_cost_usd] = ?
        WHERE [run_id] = ?
    """, counters.get('total', 0), counters.get('full', 0), counters.get('incremental', 0),
         counters.get('rebuild', 0), counters.get('thin', 0), counters.get('skipped', 0),
         counters.get('errors', 0), counters.get('total_tokens', 0), round(est_cost, 4), run_id)
    conn.commit()
    cursor.close()


# ============================================================
# UPSERT SUMMARY (Sections 2, 5, 6, 7)
# ============================================================

def upsert_summary(conn, customer_id, summary_text, summary_json,
                   last_event_ts, generation_method, validation_status,
                   token_info, escalation_risk_score, is_full_build):
    cursor = conn.cursor()
    cursor.execute("SELECT [customer_id] FROM [dbo].[LLM_Customer_Summary] WHERE [customer_id] = ?", customer_id)
    existing = cursor.fetchone()

    if existing is None:
        cursor.execute("""
            INSERT INTO [dbo].[LLM_Customer_Summary]
            ([customer_id], [rolling_summary_text], [summary_json],
             [last_event_ts], [last_processed_event_ts],
             [insert_date], [updated_date],
             [prompt_version], [model], [generation_method], [validation_status],
             [processing_status],
             [input_tokens], [output_tokens], [total_tokens], [llm_duration_ms],
             [escalation_risk_score], [last_full_build_date])
            VALUES (?, ?, ?, ?, ?, GETDATE(), GETDATE(), ?, ?, ?, ?,
                    'COMPLETED',
                    ?, ?, ?, ?,
                    ?, CASE WHEN ? = 1 THEN CAST(GETDATE() AS DATE) ELSE NULL END)
        """, customer_id, summary_text, summary_json,
             last_event_ts, last_event_ts,
             PROMPT_VERSION, LLM_CONFIG["model_name"],
             generation_method, validation_status,
             token_info.get('input_tokens'), token_info.get('output_tokens'),
             token_info.get('total_tokens'), token_info.get('duration_ms'),
             escalation_risk_score, 1 if is_full_build else 0)
        action = "INSERTED"
    else:
        cursor.execute("""
            UPDATE [dbo].[LLM_Customer_Summary]
            SET [rolling_summary_text] = ?, [summary_json] = ?,
                [last_event_ts] = ?, [last_processed_event_ts] = ?,
                [updated_date] = GETDATE(), [prompt_version] = ?, [model] = ?,
                [generation_method] = ?, [validation_status] = ?,
                [input_tokens] = ?, [output_tokens] = ?, [total_tokens] = ?,
                [llm_duration_ms] = ?,
                [escalation_risk_score] = ?,
                [last_full_build_date] = CASE
                    WHEN ? = 1 THEN CAST(GETDATE() AS DATE)
                    ELSE [last_full_build_date]
                END
            WHERE [customer_id] = ?
        """, summary_text, summary_json,
             last_event_ts, last_event_ts,
             PROMPT_VERSION, LLM_CONFIG["model_name"],
             generation_method, validation_status,
             token_info.get('input_tokens'), token_info.get('output_tokens'),
             token_info.get('total_tokens'), token_info.get('duration_ms'),
             escalation_risk_score,
             1 if is_full_build else 0,
             customer_id)
        action = "UPDATED"

    cursor.close()
    return action


# ============================================================
# MAIN PROCESSING LOOP (Sections 1-9 integrated)
# ============================================================

def process_customers(run_date, worker_id, batch_size, specific_customer=None):
    window_start = run_date - timedelta(days=WINDOW_DAYS)

    logger.info("=" * 60)
    logger.info(f"Customer 360 - LLM Summariser v3 (Enterprise)")
    logger.info(f"Prompt: {PROMPT_VERSION} | Model: {LLM_CONFIG['model_name']}")
    logger.info(f"Run Date: {run_date} | Window: {window_start} to {run_date}")
    logger.info(f"Worker: {worker_id} | Batch Size: {batch_size}")
    logger.info(f"Rebuild Interval: {REBUILD_INTERVAL_DAYS}d | Validation: {ENABLE_VALIDATION}")
    logger.info("=" * 60)

    try:
        conn = get_connection()
        client = get_llm_client()
        logger.info("Connected to DB and LLM")
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return

    # Seed PENDING from view (before claiming)
    if not specific_customer:
        seed_pending(conn)

    # Start run log
    run_id = insert_run_log(conn, run_date, window_start, worker_id)
    logger.info(f"Run log started: run_id={run_id}")

    # Counters
    cnt = {"total": 0, "inserted": 0, "updated": 0, "errors": 0, "validated": 0,
           "regenerated": 0, "thin": 0, "full": 0, "incremental": 0, "rebuild": 0,
           "skipped": 0, "total_tokens": 0, "total_input_tokens": 0, "total_output_tokens": 0}

    # Specific customer mode (bypass claim, process directly)
    if specific_customer:
        customers_to_process = [specific_customer]
        logger.info(f"Single customer mode: {specific_customer}")
    else:
        customers_to_process = None  # Use claim loop

    batch_num = 0
    while True:
        # Get next batch
        if customers_to_process is not None:
            if batch_num > 0:
                break  # Already processed the specific customer
            batch = customers_to_process
        else:
            batch = claim_batch(conn, worker_id, batch_size)
        batch_num += 1

        if not batch:
            logger.info("No more PENDING customers to claim. Exiting.")
            break

        logger.info(f"Batch {batch_num}: claimed {len(batch)} customers")

        for customer_id in batch:
            cnt["total"] += 1
            logger.info(f"[{cnt['total']}] Customer: {customer_id}")

            try:
                # ── Step 1: Determine scenario (deterministic) ──
                scenario, watermark, existing_json = determine_scenario(
                    conn, customer_id, window_start, run_date)
                logger.info(f"  Scenario: {scenario}" +
                             (f" (watermark: {watermark})" if watermark else ""))

                # ── Step 2: Build payload ──
                if scenario == 'INCREMENTAL':
                    payload = build_customer_payload(conn, customer_id, since_timestamp=watermark)
                    ds = payload["data_summary"]
                    delta_total = (ds["total_interactions"] + ds["total_call_recordings"]
                                   + ds["total_pega_cases"] + ds["total_servicenow_cases"])
                    if delta_total == 0:
                        logger.info(f"  No delta events — skipping")
                        mark_completed(conn, customer_id)
                        conn.commit()
                        cnt["skipped"] += 1
                        continue
                    logger.info(f"  Delta: {delta_total} events")
                else:
                    payload = build_customer_payload(conn, customer_id)

                payload_json = json.dumps(payload, ensure_ascii=False, default=str)
                ds = payload["data_summary"]
                logger.info(f"  Payload: {len(payload_json)} chars "
                             f"({ds['total_interactions']} int, "
                             f"{ds['recordings_with_analysis']}/{ds['total_call_recordings']} rec, "
                             f"{ds['total_pega_cases']} pega, {ds['total_servicenow_cases']} snow)")

                # ── Step 3: Thin data check (full/rebuild only) ──
                generation_method = scenario
                validation_status = "SKIPPED"
                token_info = {'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0, 'duration_ms': 0}

                if scenario != 'INCREMENTAL':
                    should_call_llm, thin_reason = has_enough_data_for_llm(payload)
                    if not should_call_llm:
                        logger.info(f"  THIN DATA — skipping LLM: {thin_reason}")
                        parsed_json = build_thin_data_summary(payload, customer_id)
                        generation_method = "THIN_DATA"
                        cnt["thin"] += 1

                        # Escalation scoring (even for thin data)
                        risk, score, risk_reason = calculate_escalation_risk(
                            parsed_json, payload.get("customer_profile"))
                        parsed_json['escalation_risk'] = risk
                        parsed_json['escalation_risk_score'] = score
                        parsed_json['escalation_risk_reason'] = risk_reason

                        # Phase 2 Enrichment for thin data
                        try:
                            # Build events_data from payload for enrichment
                            events_data = []
                            for event in payload.get("interactions", []):
                                events_data.append({
                                    "source_system": "Interaction",
                                    "event_timestamp": event.get("date"),
                                    "event_type": event.get("interaction_type"),
                                    "detail": event
                                })
                            for event in payload.get("pega_cases", []):
                                events_data.append({
                                    "source_system": "Pega",
                                    "event_timestamp": event.get("created_date"),
                                    "event_type": "case",
                                    "detail": event
                                })
                            for event in payload.get("servicenow_cases", []):
                                events_data.append({
                                    "source_system": "ServiceNow",
                                    "event_timestamp": event.get("created_date"),
                                    "event_type": "incident",
                                    "detail": event
                                })

                            # Apply enrichment using llm_enrichment module
                            enriched_json, enrichment = llm_enrichment.enrich_summary(
                                parsed_json, events_data, run_date
                            )
                            if enrichment:
                                sla = enrichment.get('sla_tracking', {})
                                if isinstance(sla, str):
                                    try:
                                        sla = json.loads(sla)
                                    except:
                                        sla = {}
                                parsed_json['sla_breaches'] = sla.get('open_cases_breached', 0) if isinstance(sla, dict) else 0
                                parsed_json['sla_breach_risk'] = sla.get('breach_risk_level', 'Unknown') if isinstance(sla, dict) else 'Unknown'

                                ces = enrichment.get('customer_effort_score', {})
                                if isinstance(ces, str):
                                    try:
                                        ces = json.loads(ces)
                                    except:
                                        ces = {}
                                parsed_json['customer_effort_score'] = ces.get('score', 0) if isinstance(ces, dict) else 0
                                parsed_json['customer_effort_band'] = ces.get('band', 'Unknown') if isinstance(ces, dict) else 'Unknown'

                                health = enrichment.get('health_score', {})
                                if isinstance(health, str):
                                    try:
                                        health = json.loads(health)
                                    except:
                                        health = {}
                                parsed_json['health_score'] = health.get('score', 0) if isinstance(health, dict) else 0
                                parsed_json['health_band'] = health.get('band', 'Unknown') if isinstance(health, dict) else 'Unknown'

                                churn = enrichment.get('churn_risk', {})
                                if isinstance(churn, str):
                                    try:
                                        churn = json.loads(churn)
                                    except:
                                        churn = {}
                                parsed_json['churn_risk'] = churn.get('churn_exposure_level', 'Unknown') if isinstance(churn, dict) else 'Unknown'
                                parsed_json['churn_risk_indicators'] = churn.get('indicators', {}) if isinstance(churn, dict) else {}
                                parsed_json['churn_risk_trajectory'] = churn.get('risk_trajectory', 'Unknown') if isinstance(churn, dict) else 'Unknown'
                        except Exception as e:
                            logger.warning(f"  Phase 2 enrichment failed (thin data): {e}")

                        # Merge programmatic fields for thin data
                        try:
                            profile = payload.get("customer_profile")
                            devices = payload.get("devices", [])
                            if profile:
                                if 'account_value' not in parsed_json:
                                    parsed_json['account_value'] = {}
                                parsed_json['account_value']['plan_count'] = profile.get('plan_count')
                                parsed_json['account_value']['account_category'] = profile.get('account_category')
                                parsed_json['account_value']['device_count'] = profile.get('device_count')
                                if profile.get('device_financing_revenue'):
                                    parsed_json['account_value']['device_financing_revenue'] = f"€{profile['device_financing_revenue']:.2f}"
                            if devices:
                                if 'account_value' not in parsed_json:
                                    parsed_json['account_value'] = {}
                                parsed_json['account_value']['devices'] = devices
                                active_contracts = sum(1 for d in devices if d.get('is_contract_active'))
                                expired_contracts = len(devices) - active_contracts
                                total_mic = sum(d.get('mic_monthly') or 0 for d in devices)
                                parsed_json['account_value']['device_portfolio'] = {
                                    'total_devices': len(devices),
                                    'active_contracts': active_contracts,
                                    'expired_contracts': expired_contracts,
                                    'total_mic_monthly': total_mic,
                                    'device_summary': f"{active_contracts} active, {expired_contracts} expired, €{total_mic:.0f}/month MIC"
                                }
                        except Exception as e:
                            logger.warning(f"  Failed to merge programmatic fields (thin data): {e}")

                        summary_text = format_summary_template(parsed_json, customer_id)
                        summary_text = summary_text.replace("{timestamp}", datetime.now().strftime("%Y-%m-%d %H:%M"))
                        summary_json_str = json.dumps(parsed_json, ensure_ascii=False, default=str)
                        last_event_ts = get_latest_event_ts(conn, customer_id)

                        upsert_summary(conn, customer_id, summary_text, summary_json_str,
                                       last_event_ts, generation_method, validation_status,
                                       token_info, score, is_full_build=True)
                        mark_completed(conn, customer_id)
                        conn.commit()
                        cnt["inserted" if scenario == 'FULL' else "updated"] += 1
                        logger.info(f"  Saved (THIN_DATA, risk={risk}, watermark={last_event_ts})")
                        continue

                # ── Step 4: Call LLM with retry ──
                if scenario == 'INCREMENTAL':
                    llm_json_text, usage, duration = call_llm_incremental(
                        client, existing_json, payload_json)
                    logger.info(f"  LLM INCREMENTAL: {duration:.1f}s, "
                                 f"{usage['total_tokens']} tokens")
                    cnt["incremental"] += 1
                else:
                    llm_json_text, usage, duration = call_llm(client, payload_json)
                    logger.info(f"  LLM FULL: {duration:.1f}s, "
                                 f"{usage['total_tokens']} tokens")
                    cnt["full" if scenario == 'FULL' else "rebuild"] += 1

                token_info = {**usage, 'duration_ms': int(duration * 1000)}
                validation_tokens = 0

                # ── Step 5: Validate ──
                if ENABLE_VALIDATION:
                    is_valid, validation_msg, val_usage = validate_summary(
                        client, payload_json, llm_json_text)
                    validation_tokens = val_usage.get('total_tokens', 0)

                    if is_valid:
                        cnt["validated"] += 1
                        validation_status = "PASSED"
                        logger.info(f"  Validation: PASSED")
                    else:
                        logger.warning(f"  Validation: FAILED — {validation_msg}")
                        llm_json_text, regen_usage = regenerate_summary(
                            client, payload_json, validation_msg)
                        cnt["regenerated"] += 1
                        # Add regen tokens to total
                        token_info['input_tokens'] += regen_usage.get('input_tokens', 0)
                        token_info['output_tokens'] += regen_usage.get('output_tokens', 0)
                        token_info['total_tokens'] += regen_usage.get('total_tokens', 0)

                        is_valid_2, _, val_usage_2 = validate_summary(
                            client, payload_json, llm_json_text)
                        validation_tokens += val_usage_2.get('total_tokens', 0)

                        if is_valid_2:
                            cnt["validated"] += 1
                            validation_status = "PASSED"
                        else:
                            validation_status = "FAILED"
                            logger.warning(f"  Re-validation: FAILED — proceeding best effort")

                # ── Step 6: Parse ──
                parsed_json, parse_ok = parse_llm_json(llm_json_text, customer_id)
                if parse_ok:
                    logger.info(f"  Parsed: sentiment={parsed_json.get('sentiment')}, "
                                 f"resolution={parsed_json.get('resolution_status')}")

                # ── Step 7: Deterministic escalation (Python, not LLM) ──
                risk, score, risk_reason = calculate_escalation_risk(
                    parsed_json, payload.get("customer_profile"))
                parsed_json['escalation_risk'] = risk
                parsed_json['escalation_risk_score'] = score
                parsed_json['escalation_risk_reason'] = risk_reason
                logger.info(f"  Escalation: {risk} (score={score})")

                # ── Phase 2 Enrichment (SLA, CES, Health Score, Churn Risk) ──
                try:
                    # Build events_data from payload for enrichment
                    events_data = []
                    for event in payload.get("interactions", []):
                        events_data.append({
                            "source_system": "Interaction",
                            "event_timestamp": event.get("date"),
                            "event_type": event.get("interaction_type"),
                            "detail": event
                        })
                    for event in payload.get("pega_cases", []):
                        events_data.append({
                            "source_system": "Pega",
                            "event_timestamp": event.get("created_date"),
                            "event_type": "case",
                            "detail": event
                        })
                    for event in payload.get("servicenow_cases", []):
                        events_data.append({
                            "source_system": "ServiceNow",
                            "event_timestamp": event.get("created_date"),
                            "event_type": "incident",
                            "detail": event
                        })

                    # Apply enrichment using llm_enrichment module
                    enriched_json, enrichment = llm_enrichment.enrich_summary(
                        parsed_json, events_data, run_date
                    )
                    if enrichment:
                        # Add key enrichment fields to summary
                        sla = enrichment.get('sla_tracking', {})
                        if isinstance(sla, str):
                            try:
                                sla = json.loads(sla)
                            except:
                                sla = {}
                        parsed_json['sla_breaches'] = sla.get('open_cases_breached', 0) if isinstance(sla, dict) else 0
                        parsed_json['sla_breach_risk'] = sla.get('breach_risk_level', 'Unknown') if isinstance(sla, dict) else 'Unknown'

                        ces = enrichment.get('customer_effort_score', {})
                        if isinstance(ces, str):
                            try:
                                ces = json.loads(ces)
                            except:
                                ces = {}
                        parsed_json['customer_effort_score'] = ces.get('score', 0) if isinstance(ces, dict) else 0
                        parsed_json['customer_effort_band'] = ces.get('band', 'Unknown') if isinstance(ces, dict) else 'Unknown'

                        health = enrichment.get('health_score', {})
                        if isinstance(health, str):
                            try:
                                health = json.loads(health)
                            except:
                                health = {}
                        parsed_json['health_score'] = health.get('score', 0) if isinstance(health, dict) else 0
                        parsed_json['health_band'] = health.get('band', 'Unknown') if isinstance(health, dict) else 'Unknown'

                        churn = enrichment.get('churn_risk', {})
                        if isinstance(churn, str):
                            try:
                                churn = json.loads(churn)
                            except:
                                churn = {}
                        parsed_json['churn_risk'] = churn.get('churn_exposure_level', 'Unknown') if isinstance(churn, dict) else 'Unknown'
                        parsed_json['churn_risk_indicators'] = churn.get('indicators', {}) if isinstance(churn, dict) else {}
                        parsed_json['churn_risk_trajectory'] = churn.get('risk_trajectory', 'Unknown') if isinstance(churn, dict) else 'Unknown'

                        logger.info(f"  Enrichment: CES={ces.get('band') if isinstance(ces, dict) else 'Unknown'}, "
                                   f"Health={health.get('band') if isinstance(health, dict) else 'Unknown'}, "
                                   f"Churn={churn.get('churn_exposure_level') if isinstance(churn, dict) else 'Unknown'}")
                except Exception as e:
                    import traceback
                    logger.warning(f"  Phase 2 enrichment failed: {e}")
                    logger.warning(f"  Traceback: {traceback.format_exc()}")

                # ── Step 7c: Merge programmatic fields (devices, revenue details) ──
                # These fields are populated from Revenue_Cache and Customer_Device_Assets
                # and need to be merged into the final summary_json_str after LLM generation
                try:
                    final_json = json.loads(json.dumps(parsed_json, ensure_ascii=False, default=str))
                    profile = payload.get("customer_profile")

                    # Add revenue and device fields from profile
                    if profile:
                        # Update account_value with revenue cache fields
                        if 'account_value' not in final_json:
                            final_json['account_value'] = {}

                        final_json['account_value']['plan_count'] = profile.get('plan_count')
                        final_json['account_value']['account_category'] = profile.get('account_category')
                        final_json['account_value']['device_count'] = profile.get('device_count')

                        if profile.get('device_financing_revenue'):
                            final_json['account_value']['device_financing_revenue'] = f"€{profile['device_financing_revenue']:.2f}"

                    # Add devices array and device portfolio summary
                    devices = payload.get("devices", [])
                    if devices:
                        if 'account_value' not in final_json:
                            final_json['account_value'] = {}

                        final_json['account_value']['devices'] = devices

                        # Calculate device portfolio summary
                        active_contracts = sum(1 for d in devices if d.get('is_contract_active'))
                        expired_contracts = len(devices) - active_contracts
                        total_mic = sum(d.get('mic_monthly') or 0 for d in devices)

                        device_portfolio = {
                            'total_devices': len(devices),
                            'active_contracts': active_contracts,
                            'expired_contracts': expired_contracts,
                            'total_mic_monthly': total_mic,
                            'device_summary': f"{active_contracts} active, {expired_contracts} expired, €{total_mic:.0f}/month MIC"
                        }
                        final_json['account_value']['device_portfolio'] = device_portfolio

                    # Update parsed_json with merged fields
                    parsed_json = final_json
                except Exception as e:
                    logger.warning(f"  Failed to merge programmatic fields: {e}")

                # ── Step 8: Format + Save (atomic) ──
                summary_text = format_summary_template(parsed_json, customer_id)
                summary_text = summary_text.replace("{timestamp}", datetime.now().strftime("%Y-%m-%d %H:%M"))
                summary_json_str = json.dumps(parsed_json, ensure_ascii=False, default=str)
                last_event_ts = get_latest_event_ts(conn, customer_id)

                is_full = scenario in ('FULL', 'REBUILD')
                action = upsert_summary(conn, customer_id, summary_text, summary_json_str,
                                        last_event_ts, generation_method, validation_status,
                                        token_info, score, is_full_build=is_full)

                # ── Step 9: Token log + state transition ──
                append_token_log(conn, customer_id, run_date, scenario,
                                 token_info, token_info.get('duration_ms', 0), validation_tokens)
                mark_completed(conn, customer_id)
                conn.commit()  # Single atomic commit

                cnt["inserted" if action == "INSERTED" else "updated"] += 1
                cnt["total_tokens"] += token_info.get('total_tokens', 0) + validation_tokens
                cnt["total_input_tokens"] += token_info.get('input_tokens', 0)
                cnt["total_output_tokens"] += token_info.get('output_tokens', 0)
                logger.info(f"  {action} ({generation_method}, risk={risk}, watermark={last_event_ts})")

            except Exception as e:
                import traceback
                logger.error(f"  FAILED: {e}")
                logger.error(f"  Traceback: {traceback.format_exc()}")
                conn.rollback()
                mark_failed(conn, customer_id, str(e))
                conn.commit()
                cnt["errors"] += 1

            time.sleep(0.3)

    # Finalise
    finalise_run_log(conn, run_id, cnt)

    logger.info("=" * 60)
    logger.info("PROCESSING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Total Processed      : {cnt['total']}")
    logger.info(f"Inserted (new)       : {cnt['inserted']}")
    logger.info(f"Updated              : {cnt['updated']}")
    logger.info(f"Scenario FULL        : {cnt['full']}")
    logger.info(f"Scenario INCREMENTAL : {cnt['incremental']}")
    logger.info(f"Scenario REBUILD     : {cnt['rebuild']}")
    logger.info(f"Thin-data (no LLM)   : {cnt['thin']}")
    logger.info(f"Skipped (no delta)   : {cnt['skipped']}")
    logger.info(f"Validated (passed)   : {cnt['validated']}")
    logger.info(f"Regenerated          : {cnt['regenerated']}")
    logger.info(f"Errors               : {cnt['errors']}")
    logger.info(f"Total Tokens         : {cnt['total_tokens']:,}")
    est_cost = (
        cnt['total_input_tokens'] * COST_PER_1M_INPUT / 1_000_000
        + cnt['total_output_tokens'] * COST_PER_1M_OUTPUT / 1_000_000
    )
    logger.info(f"Estimated Cost       : ${est_cost:.4f} USD")
    processed = cnt['inserted'] + cnt['updated']
    if cnt['total'] > 0:
        logger.info(f"Success Rate         : {processed/cnt['total']*100:.1f}%")
    logger.info(f"Run ID               : {run_id}")
    logger.info("=" * 60)

    conn.close()


# ============================================================
# STATUS CHECK (updated for enterprise schema)
# ============================================================

def check_status():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM [dbo].[LLM_Customer_Summary]")
    total_summaries = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT [customer_id]) FROM [dbo].[Customer360_Events] WHERE [is_deleted] = 0")
    total_customers = cursor.fetchone()[0]

    print("\n" + "=" * 65)
    print("  CUSTOMER 360 — LLM Summariser v3 (Enterprise) Status")
    print("=" * 65)
    print(f"  Customers in Events table  : {total_customers}")
    print(f"  Summaries in Summary table : {total_summaries}")

    # Processing status
    cursor.execute("""
        SELECT [processing_status], COUNT(*), AVG([retry_count])
        FROM [dbo].[LLM_Customer_Summary]
        GROUP BY [processing_status] ORDER BY [processing_status]
    """)
    print("\n  Processing Status:")
    for row in cursor.fetchall():
        print(f"    {row[0]:15s}: {row[1]:>6d}  (avg retries: {row[2] or 0:.1f})")

    # Summary status from view
    cursor.execute("""
        SELECT [summary_status], COUNT(*) FROM [dbo].[vw_CustomersPendingSummary]
        GROUP BY [summary_status] ORDER BY [summary_status]
    """)
    print("\n  Summary Freshness:")
    pending = 0
    for row in cursor.fetchall():
        marker = "  " if row[0] == "UP_TO_DATE" else "→ "
        print(f"    {marker}{row[0]:25s}: {row[1]:>6d}")
        if row[0] != "UP_TO_DATE": pending += row[1]
    if total_customers > 0:
        print(f"\n  Needing processing: {pending}  |  Completion: {((total_customers - pending) / total_customers * 100):.1f}%")

    # Generation method
    cursor.execute("""
        SELECT [generation_method], COUNT(*) FROM [dbo].[LLM_Customer_Summary]
        WHERE [generation_method] IS NOT NULL
        GROUP BY [generation_method] ORDER BY [generation_method]
    """)
    print("\n  By Generation Method:")
    for row in cursor.fetchall():
        print(f"    {row[0]:15s}: {row[1]}")

    # Token costs (last 7 days)
    cursor.execute("""
        SELECT [run_date], [scenario], COUNT(*), SUM([total_tokens]),
               CAST(SUM([input_tokens]) * 2.50 / 1000000.0
                  + SUM([output_tokens]) * 10.00 / 1000000.0 AS DECIMAL(10,4))
        FROM [dbo].[LLM_Token_Log]
        WHERE [run_date] >= DATEADD(DAY, -7, GETDATE())
        GROUP BY [run_date], [scenario]
        ORDER BY [run_date] DESC, [scenario]
    """)
    rows = cursor.fetchall()
    if rows:
        print("\n  Token Usage (Last 7 Days):")
        for row in rows:
            print(f"    {row[0]} | {row[1]:12s} | {row[2]:>4d} calls | {row[3]:>8,d} tokens | ${row[4]:.4f}")

    # Recent run logs
    cursor.execute("""
        SELECT TOP 5 [run_id], [run_date], [status], [total_customers],
               [scenario_full], [scenario_incr], [scenario_rebuild],
               [errors], [total_tokens], [estimated_cost_usd],
               [started_at], [completed_at]
        FROM [dbo].[LLM_Run_Log]
        ORDER BY [run_id] DESC
    """)
    runs = cursor.fetchall()
    if runs:
        print("\n  Recent Runs:")
        for r in runs:
            duration = ""
            if r[10] and r[11]:
                dur_sec = (r[11] - r[10]).total_seconds()
                duration = f" ({dur_sec/60:.0f}min)"
            print(f"    Run {r[0]}: {r[1]} | {r[2]}{duration} | "
                  f"F:{r[4] or 0} I:{r[5] or 0} R:{r[6] or 0} | "
                  f"Err:{r[7] or 0} | {r[8] or 0:,} tok | ${r[9] or 0:.4f}")

    # Stuck rows
    cursor.execute("""
        SELECT COUNT(*) FROM [dbo].[LLM_Customer_Summary]
        WHERE [processing_status] = 'IN_PROGRESS'
          AND [last_attempted_at] < DATEADD(MINUTE, -?, GETDATE())
    """, STUCK_TIMEOUT_MINUTES)
    stuck = cursor.fetchone()[0]
    if stuck > 0:
        print(f"\n  ⚠ STUCK IN_PROGRESS: {stuck} rows (>{STUCK_TIMEOUT_MINUTES} min old)")

    # Failed with max retries
    cursor.execute("""
        SELECT COUNT(*) FROM [dbo].[LLM_Customer_Summary]
        WHERE [processing_status] = 'FAILED' AND [retry_count] >= 3
    """)
    exhausted = cursor.fetchone()[0]
    if exhausted > 0:
        print(f"  ⚠ EXHAUSTED RETRIES: {exhausted} customers (retry_count >= 3)")

    print("\n" + "=" * 65)
    cursor.close()
    conn.close()


# ============================================================
# RETRY ALL
# ============================================================

def retry_all(run_date, worker_id, batch_size):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE [dbo].[LLM_Customer_Summary]
        SET [rolling_summary_text] = NULL, [summary_json] = NULL,
            [last_processed_event_ts] = NULL, [last_full_build_date] = NULL,
            [processing_status] = 'PENDING', [retry_count] = 0,
            [error_message] = NULL, [worker_id] = NULL
    """)
    reset_count = cursor.rowcount
    conn.commit()
    cursor.close()
    # FIX: Removed duplicate conn.close() - connection will be closed in process_customers

    logger.info(f"Reset {reset_count} customers (cleared all → forces Scenario 1 FULL)")
    if reset_count > 0:
        process_customers(run_date, worker_id, batch_size)


# ============================================================
# ENTRY POINT
# ============================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="Customer 360 — LLM Summariser v3 (Enterprise)",
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('command', nargs='?', default='run',
                        choices=['run', 'status', 'retry', 'customer', 'help'],
                        help="Command to execute")
    parser.add_argument('customer_id', nargs='?', default=None,
                        help="Customer ID (for 'customer' command)")
    parser.add_argument('--run-date', type=str, default=None,
                        help="Run date YYYY-MM-DD (default: today)")
    parser.add_argument('--worker-id', type=str, default=None,
                        help="Worker ID for concurrency (default: hostname-PID)")
    parser.add_argument('--batch-size', type=int, default=BATCH_SIZE,
                        help=f"Batch claim size (default: {BATCH_SIZE})")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # Resolve run_date (deterministic, Section 1)
    if args.run_date:
        try:
            run_date = datetime.strptime(args.run_date, "%Y-%m-%d").date()
        except ValueError:
            print(f"ERROR: Invalid date format: {args.run_date} (expected YYYY-MM-DD)")
            sys.exit(1)
    else:
        run_date = date.today()

    # Resolve worker_id (Section 9)
    worker_id = args.worker_id or f"{socket.gethostname()}-{os.getpid()}"

    if args.command == "status":
        check_status()
    elif args.command == "retry":
        retry_all(run_date, worker_id, args.batch_size)
    elif args.command == "customer":
        if not args.customer_id:
            print("ERROR: Specify customer ID, e.g.: python llm_summariser_v4.py customer 12345")
            sys.exit(1)
        process_customers(run_date, worker_id, args.batch_size, specific_customer=args.customer_id)
    elif args.command == "help":
        print("""
Customer 360 — LLM Summariser v4 (Enterprise)
================================================================
Prompt: v6.0-enterprise | Data: Customer360_Events + CallTranscript + Revenue_Cache

Scenarios:
  1. FULL        — No existing summary → full 30d → INSERT
  2. INCREMENTAL — Valid watermark → delta only → merge → UPDATE
  3. REBUILD     — Stale watermark OR >30d since full → full 30d → UPDATE

Enterprise Features:
  - Deterministic window from --run-date (not datetime.now)
  - Processing state machine (PENDING → IN_PROGRESS → COMPLETED/FAILED)
  - Revenue from Revenue_Cache (no live IEROXAPP2 dependency)
  - Exponential backoff retry for Azure API
  - Token governance (LLM_Token_Log, per-call capture)
  - Deterministic escalation risk scoring (Python, not LLM)
  - Periodic rebuild every 30 days (anti-drift)
  - Run logging (LLM_Run_Log)
  - Concurrency-safe (--worker-id, atomic claim pattern)

Usage:
    python llm_summariser_v4.py                                     Process all
    python llm_summariser_v4.py --run-date 2026-02-17               Specific date
    python llm_summariser_v4.py --worker-id A --batch-size 20       Parallel worker
    python llm_summariser_v4.py status                              Check status
    python llm_summariser_v4.py retry                               Reprocess ALL
    python llm_summariser_v4.py customer 12345                      One customer

Daily Pipeline:
    01:00  python refresh_revenue_cache.py    (IEROXAPP2 → Revenue_Cache)
    02:00  EXEC sp_Customer360_ETL            (sliding 30-day load)
    02:30  python load_transcripts_v2.py       (transcript JSON)
    03:00  python llm_summariser_v4.py --run-date 2026-02-17
    03:01  python llm_summariser_v4.py status
        """)
    else:
        process_customers(run_date, worker_id, args.batch_size)
