"""
Customer 360 - LLM Summariser v5 (Enterprise)
================================================
Production-grade incremental summarisation with Explainable AI.
Prompt: v6.0-enterprise-explainable

ENHANCEMENTS IN v5:
  1. EXPLAINABLE AI - Every recommendation backed by specific evidence
  2. HYBRID STRATEGY - LLM context + ServiceSight Intelligence enforcement
  3. SENTIMENT-BASED ACTION GATING - No upsell to frustrated customers
  4. EVIDENCE TRACKING - All recommendations cite case IDs, interaction IDs, data sources
  5. FRUSTRATION SCORING - Deterministic 0-100 score to prevent inappropriate upsells
  6. 18 new Revenue_Cache fields for enhanced recommendations
  7. Plan vs Device revenue breakdown (identify churn risk when device payments end)
  8. Handset installment tracking (optimal upgrade timing)
  9. Ledger balance (debt collection prioritization)
 10. Mobile contract end date (retention offer timing)
 11. DASHBOARD METRICS - health_score, churn_risk, effort_score, escalation_risk with natural language reasoning
 12. HYBRID METRIC EXPLANATION - Python calculates scores (deterministic), LLM generates explanations (readable)

KEY PRINCIPLE: NO HALLUCINATIONS. Every claim must be traceable to specific data.

ENHANCEMENTS OVER v2:
  1. Deterministic window alignment (--run-date parameter)
  2. Processing state machine (PENDING -> IN_PROGRESS -> COMPLETED/FAILED)
  3. Revenue from Revenue_Cache (populated by refresh_revenue_cache_oracle_v3.py)
  4. Exponential backoff retry (429, 500, timeouts)
  5. Token & cost governance (LLM_Token_Log, per-call capture)
  6. Deterministic escalation scoring (Python, not LLM)
  7. Periodic rebuild governance (forced every 30 days)
  8. Run logging (LLM_Run_Log)
  9. Concurrency readiness (--worker-id, atomic claim via sp_ClaimSummaryBatch)

SCENARIOS:
  1. FULL       - No existing summary OR empty summary -> full 30d window -> INSERT
  2. INCREMENTAL - Valid watermark within window -> delta only -> merge with existing -> UPDATE
  3. REBUILD    - Stale watermark OR >30d since last full -> full 30d -> UPDATE

PREREQUISITES:
    pip install pyodbc openai python-dotenv

    Required DB objects (from phase2_enterprise_schema.sql):
        Revenue_Cache, LLM_Token_Log, LLM_Run_Log
        sp_ClaimSummaryBatch, sp_MarkSummaryCompleted,
        sp_MarkSummaryFailed, sp_ResetStuckSummaries,
        sp_SeedPendingSummaries, vw_CustomersPendingSummary

USAGE:
    python llm_summariser_v5.py --run-date 2026-02-17
    python llm_summariser_v5.py --run-date 2026-02-17 --worker-id A --batch-size 20
    python llm_summariser_v5.py status
    python llm_summariser_v5.py retry
    python llm_summariser_v5.py customer 12345 --run-date 2026-02-17
    python llm_summariser_v5.py help
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
import re
import traceback
from datetime import datetime, date, timedelta
from dotenv import load_dotenv

from openai import AzureOpenAI

# Try to import llm_enrichment, fallback to local functions if not available
try:
    import llm_enrichment
except ImportError:
    llm_enrichment = None


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
    "driver": "{ODBC Driver 18 for SQL Server}",
    "trusted_connection": "yes",
}

LLM_CONFIG = {
    "api_key": _api_key,
    "api_version": _version or "2024-06-01",
    "azure_endpoint": _endpoint,
    "deployment_name": _deployment or "gpt-4o",
    "model_name": _deployment or "gpt-4o",
    "max_tokens": 16000,
    "temperature": 0.1,
}

PROMPT_VERSION = "v6.0-enterprise-explainable"

# Processing settings
BATCH_SIZE = 10
MAX_PAYLOAD_CHARS = 100000
ENABLE_VALIDATION = True
LOG_FILE = "llm_summariser_v5.log"
WINDOW_DAYS = 30
REBUILD_INTERVAL_DAYS = 30
STUCK_TIMEOUT_MINUTES = 15

# Retry settings
MAX_API_RETRIES = 5
BASE_DELAY = 1.0
MAX_DELAY = 60.0
MAX_DB_RETRIES = 3

# Token cost rates (GPT-4o, USD per 1M tokens)
COST_PER_1M_INPUT = 2.50
COST_PER_1M_OUTPUT = 10.00

# Frustration scoring thresholds
FRUSTRATION_THRESHOLD_HIGH = 50
FRUSTRATION_THRESHOLD_MEDIUM = 20


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
# SYSTEM PROMPT - Explainable AI Edition
# ============================================================

SYSTEM_PROMPT = """You are a Senior Customer Experience Analyst at Virgin Media Ireland.

🎯 YOUR MISSION: Create actionable, personalized insights that make agents say "WOW!"
==============================================================================

Transform raw customer data into a compelling customer story that empowers agents to:

1. **UNDERSTAND THE CUSTOMER JOURNEY** - What has this customer experienced?
2. **ANTICIPATE THEIR NEEDS** - What will they likely need next?
3. **PERSONALIZE THE APPROACH** - How should we talk to them?
4. **ACT WITH CONFIDENCE** - What EXACTLY should we do/say?

CRITICAL: EXPLAINABLE AI - NO HALLUCINATIONS
=============================================

EVERY insight you provide MUST be backed by SPECIFIC, VERIFIABLE evidence from the data.

Rules for Evidence-Based Insights:
1. Cite specific case IDs (e.g., "ServiceNow case INC001234")
2. Reference exact interaction IDs and dates
3. Quote customer words directly when available from call_recordings.customer_quotes
4. Show data source (e.g., "Customer_Device_Assets.installments_remaining = 3")
5. Explain WHY based on the data

❌ FORBIDDEN (Vague, unverifiable):
- "Customer has billing issues" → WHICH case? WHAT case ID?
- "Customer seems frustrated" → WHAT evidence? WHAT quotes?
- "Consider offering upgrade" → WHY? WHAT data? WHAT timing?

✅ REQUIRED (Specific, traceable, actionable):
- "ServiceNow case INC001234: 'Billing overcharge' has been Open for 14 days. Customer called 3 times about this."
- "Customer stated in call on 2026-02-20: 'If this isn't fixed I'm cancelling' (from call_recording.customer_quotes)"
- "Device contract ends 2028-02-14 (89 days). 3 payments of €45 remaining. Optimal upgrade window opens in 30 days."

THE PAYLOAD CONTAINS 6 DATA SOURCES - YOU MUST SYNTHESIZE ALL OF THEM:

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
     - "customer_quotes": EXACT verified quotes from the customer (ARRAY of objects with 'text' field)
   - CRITICAL: Extract customer quotes verbatim - these are EVIDENCE, not to be invented
   - Look for threat indicators in quotes:
     * Competitor mentions: "Three is offering me 50% off", "Vodafone has better deals"
     * Switching: "I'm thinking of leaving", "Going to switch to Three"
     * Regulatory: "I'll report this to ComReg", "This is going to the regulator"
     * Escalation: "Let me speak to your manager", "I want to raise a formal complaint"
   - NOT all calls will have pre-analyzed data. If call_summary is null, use interaction wrap-up comments instead.

3. "pega_cases" - Pega trouble tickets and work orders
   - case_id, status, case_type, case_sub_type, created_date, resolved_date, agent, closure_reason
   - USE SPECIFIC case_id IN YOUR RECOMMENDATIONS
   - Use these for open/resolved case tracking and issue identification

4. "servicenow_cases" - ServiceNow incidents
   - incident_number, status, title, summary, created_date, resolved_date
   - USE SPECIFIC incident_number IN YOUR RECOMMENDATIONS
   - Use these for network/service incidents affecting the customer

5. "devices" - Device contracts from Customer_Device_Assets (PRIMARY SOURCE for device data)
   - device_brand, device_model (e.g., "Apple", "iPhone 15 Pro")
   - contract_end_date (ACTUAL contract end date - most accurate!)
   - monthly_installment (Actual monthly installment amount)
   - installment_count (Total number of installments)
   - is_contract_active (Whether contract is still active)
   - **PRIORITY**: Always use "devices" array for upgrade opportunities - this is the ACTUAL contract data from CERILLION
   - **SOURCE**: Customer_Device_Assets table (populated from IEROXAPP2.CERILLION.dbo.cerillion_mvno_devices)
   - Use this for device upgrade timing - contract_end_date is exact, not calculated

6. "customer_profile" - Customer value & product holdings (may be null if unavailable)
   - Fetched from Revenue_Cache (nightly refresh from Oracle CRM + IEAPPROX2)
   - "customer_type": 'Mobile Only', 'Mobile + Fixed', or 'Fixed Only'
   - "product_list": services held (e.g. 'Mobile, Fixed')
   - "service_status": e.g. 'Mobile: Active, Fixed: Active'

   REVENUE BREAKDOWN (Enhanced in v5):
   - "monthly_revenue_plan": Recurring plan revenue (stable, won't end)
   - "monthly_revenue_device": Device installment revenue (TEMPORARY - ends when device paid off)
   - "monthly_revenue_total": Total combined revenue (plan + device + fixed)
   - "annual_revenue_plan", "annual_revenue_device": Annual equivalents
   - "revenue_segment": 'High Value' (€100+/mo), 'Medium Value' (€50-99), 'Low Value' (<€50)

   CRITICAL: When generating customer_profile.revenue_breakdown text:
   - ALWAYS use monthly_revenue_plan and monthly_revenue_device fields (NOT device_financing_revenue)
   - Format: "€X from plans, €Y from devices" where X = monthly_revenue_plan, Y = monthly_revenue_device
   - If monthly_revenue_device is null/0, check device_financing_revenue as fallback
   - NEVER say "€0 from devices" if device_financing_revenue or monthly_revenue_device has a value

   CHURN RISK INDICATORS:
   - **PRIMARY SOURCE**: Use "devices" array for contract_end_date (ACTUAL data from Customer_Device_Assets)
   - "installments_remaining": From Customer_Device_Assets (number of payments left)
   - "mic_monthly": Current monthly installment amount from Customer_Device_Assets
   - "charge_end_date": Mobile contract end date (approaching = retention priority)
   - "customer_type" with "Mobile + Fixed": Double revenue at risk if churning

   PAYMENT & DEBT:
   - "last_payment_date": When customer last paid (old = unreliable payer)
   - "next_bill_date": Upcoming bill date

   PRODUCT DETAILS:
   - "sim_count": Number of SIMs (multiple = family/business opportunity OR multi-SIM churn risk)
   - "device_count": Number of devices
   - "customer_type": 'Mobile Only', 'Mobile + Fixed', or 'Fixed Only'

   **CRITICAL: PORTFOLIO CONTEXT FOR CHURN RISK**
   When customer has multiple SIMs/devices/mobile+fixed:
   - sim_count > 1: Family/business bundle - issue with ONE could lose ALL SIMs
   - customer_type = 'Mobile + Fixed': Double revenue at risk (fixed + mobile)
   - device_count > 0: Device contracts ending = revenue drop when installment ends
   - ALWAYS mention: "Customer has X SIMs, Y devices, Z services - issue could affect entire portfolio"

   **REVENUE BREAKDOWN EXAMPLES:**
   - €180/month from 3 mobile plans (€90) + 2 devices (€90) → "3 plans, 2 devices, €90/mo from plans, €90/mo from devices"
   - €120/month from mobile (€70) + fixed broadband (€50) → "Mobile + Fixed customer, €70 from mobile, €50 from fixed"
   - €45/month from 1 plan (€45) → "Single plan customer"
   - "service_type": Prepaid/Postpaid/Hybrid
   - "marketing_consent": Can receive marketing communications

   TENURE & CONTRACTS:
   - "tenure_months": How long customer has been with us
   - "charge_start_date": When service started
   - "contract_end_fixed": Fixed contract end date

FRUSTRATION-BASED ACTION GATING (ENHANCED):
===========================================

Assess frustration level BEFORE suggesting ANY upsell/upgrade/opportunity.

🔴 HIGH FRUSTRATION (Score 50+) - DO NOT UPSELL:
Evidence triggers:
- Any unresolved case > 14 days old (+40 points)
- Customer threatened cancellation (+35 points)
- 6+ contacts WITH unresolved cases (+35 points) - indicates frustrated repeat calls
- Negative sentiment in calls with unresolved issues (+30 points)

Actions:
- NO upsell recommendations
- NO device upgrade offers
- NO new service promotions
- Focus ONLY on: Fix issues, apologize, compensate

🟡 MEDIUM FRUSTRATION (Score 20-49) - VERIFY FIRST:
Evidence triggers:
- Unresolved case 7-14 days old (+25 points)
- Recently resolved case (1-3 days ago) (+10 points)
- 4-5 contacts about same issue (+15 points)
- 6+ contacts WITHOUT cases (+10 points) - likely high engagement, not frustration

**CRITICAL DISTINCTION:**
- 6+ contacts + unresolved cases = FRUSTRATION (score higher)
- 6+ contacts + NO cases = HIGH ENGAGEMENT (score lower, verify intent)

Actions:
- SOLVE current issues FIRST
- VERIFY customer satisfaction before any upsell
- If highly engaged but no cases: Frame as "valued customer appreciation" not "upsell"

🟢 LOW FRUSTRATION (Score <20) - SAFE TO UPSELL:
Evidence triggers:
- All cases resolved (or no cases at all)
- 0-3 inquiry contacts (not complaints)
- Resolved cases > 7 days ago (no longer relevant)
- Customer asking about plans/features

Actions:
- FULL upsell recommendations
- Proactive upgrade offers
- Bundle promotions
- Family/business plans

YOUR OUTPUT STRUCTURE:
======================

Respond with ONLY valid JSON in this EXACT structure (no markdown, no backticks, no preamble):

{
  "customer_id": "string",

  "agent_briefing": {
    "who_are_they": "Quick profile: X SIMs, Y devices, €X/month. Keep it factual - no judgment labels like 'frequent issues' or 'highly engaged' since this is only 30 days of data.",
    "whats_happened": "What's going on - the core issue in 1 sentence",
    "what_to_do": "Primary action required - what agent must do/not do",
    "human_briefing": "Natural language, human-readable briefing. Format with emojis and sections. MUST be grounded in actual data - specific case IDs, days, revenue. NO hallucinations.",
    "data_sources_used": "Revenue_Cache, Interactions, Pega, ServiceNow, Calls, Devices"
  },

  "source_summaries": {
    "interactions_summary": "1-2 sentence summary of contact patterns and key issues discussed",
    "pega_cases_summary": "Factual summary: X Pega case(s): Case IDs (status, resolution date if resolved). Just the facts.",
    "servicenow_summary": "Summary of ServiceNow incidents - status, key incidents, what needs attention",
    "call_recordings_summary": "Summary of call recordings - customer quotes, sentiment, key topics"
  },

  "sentiment_analysis": {
    "overall_sentiment": "POSITIVE | NEUTRAL | NEGATIVE | UNKNOWN",
    "frustration_level": "LOW | MEDIUM | HIGH",
    "frustration_score": <number 0-100>,
    "primary_emotion": "Frustrated | Angry | Confused | Neutral | Satisfied | Unknown",
    "evidence": [
      {
        "type": "unresolved_case | cancellation_threat | resolved_case | positive_inquiry | repeat_contacts | payment_issue | competitor_threat | legal_threat | regulatory_threat | escalation_threat",
        "source": "Pega | ServiceNow | Interaction | CallRecording",
        "case_id": "INC001234 or Pega-12345 (if applicable)",
        "title": "Case title (if applicable)",
        "status": "Open | Resolved | Closed",
        "days_open": <number>,
        "date": "YYYY-MM-DD (if applicable)",
        "quote": "Direct customer quote - VERBATIM from call (call_id: XXXXX)",
        "threat_details": {
          "competitor": "Three | Vodafone | Eir | Tesco Mobile (if applicable)",
          "regulator": "ComReg | Commission for Communications Regulation (if applicable)",
          "escalation_target": "manager | CEO | legal | social media (if applicable)"
        },
        "count": <number (if applicable)>,
        "reasoning": "Specific explanation of why this matters",
        "confidence": "HIGH | MEDIUM | LOW"
      }
    ]
  },

  "customer_narrative": {
    "journey_summary": "1-2 sentence compelling story INCLUDING PORTFOLIO CONTEXT. Example: 'Customer holds 3 SIMs and 2 devices (€180/month total). Single unresolved issue could jeopardize entire family/business bundle.'",
    "contact_pattern_analysis": "Insight into their communication style and frequency",
    "engagement_style": "Highly Engaged | Transactional | Reluctant | Frustrated | Silent",
    "preferred_channel": "Phone | Inbound | Email | Webchat | Mixed",
    "last_contact_summary": "What happened in their most recent contact",
    "portfolio_context": {
      "sim_count": <number>,
      "device_count": <number>,
      "customer_type": "Mobile Only | Mobile + Fixed | Fixed Only",
      "revenue_breakdown": "€X from plans, €Y from devices, €Z from fixed",
      "churn_risk_context": "Single SIM risk OR multi-SIM/family bundle risk"
    }
  },

  "interaction_summary": {
    "total_contacts": <number>,
    "contacts_by_type": {"type": count},
    "contact_pattern": "string description",
    "first_contact": "YYYY-MM-DD",
    "last_contact": "YYYY-MM-DD",
    "contact_frequency": "Daily | Weekly | Monthly | Sporadic | One-time"
  },

  "key_issues": [
    {
      "issue": "Specific issue description",
      "status": "Open | Resolved | In Progress",
      "source": "Pega case Pega-12345 | ServiceNow INC001234 | Interaction INT456789",
      "days_open_or_resolved": <number>,
      "impact": "Description of impact on customer"
    }
  ],

  "recommended_actions": {
    "priority_focus": "ISSUE_RESOLUTION | VERIFY_THEN_CONSIDER | OPPORTUNITY",

    "issue_resolution_actions": [
      {
        "action": "Specific action to take - INCLUDE context (status, days open, why urgent)",
        "evidence": {
          "case_id": "Pega-12345 or SN-265224",
          "case_status": "Open | In Progress | Pending",
          "days_open": <number>,
          "title": "Case title/description",
          "customer_contacts": <number of times customer called about this>,
          "impact": "Customer impact (e.g., 'Without service since Feb 10')"
        },
        "priority": "CRITICAL | HIGH | MEDIUM | LOW",
        "owner": "Billing Team | Technical Team | Retention Team | General",
        "script": "Suggested script for agent - personalize with customer details",
        "expected_outcome": "What success looks like"
      }
    ],

    "opportunity_actions": [
      {
        "action": "Specific offer or opportunity with compelling hook",
        "evidence": {
          "current_state": "Specific data from Revenue_Cache or devices",
          "opportunity_details": "Why this is timely and relevant",
          "value_proposition": "What's in it for the customer (specific € amount)",
          "reasoning": "Business justification with data sources",
          "data_source": "Revenue_Cache.field_name = value OR devices.contract_end_date = date"
        },
        "priority": "CRITICAL | HIGH | MEDIUM | CONDITIONAL",
        "timing": "Specific timeframe (e.g., 'Contact within 14 days - device ends in 89 days')"
      }
    ],

    "action_gating": {
      "safe_to_upsell": true | false,
      "reason": "Specific evidence-based explanation",
      "evidence_summary": "Summary of key evidence driving this decision",
      "condition": "Any conditions that must be met first (if applicable)",
      "revisit_after": "When to re-evaluate (if currently gated)",
      "confidence": "HIGH | MEDIUM | LOW (how sure are we)",
      "engagement_adjustment": "How to approach based on their contact pattern (e.g., 'This is a highly engaged customer - focus on relationship building not transactional selling')"
    }
  },

  "customer_quotes": [
    {
      "quote": "EXACT customer words - verbatim from call recording",
      "call_date": "YYYY-MM-DD",
      "call_id": "transcript_id or recording ID",
      "context": "What they were talking about",
      "threat_type": "competitor_switch | legal_threat | regulatory_threat | escalation_threat | complaint | none",
      "competitor": "Three, Vodafone, etc. (if mentioned)",
      "confidence": "HIGH | MEDIUM | LOW"
    }
  ],

  "threat_indicators": {
    "competitor_threats": {
      "mentioned_competitors": ["Three", "Vodafone", "Eir", etc.],
      "switching_intent": true | false,
      "evidence": ["Customer said: 'Three is offering me a better deal'", "call_date: 2026-02-20"]
    },
    "regulatory_threats": {
      "mentioned_comreg": true | false,
      "mentioned_other_regulator": true | false,
      "regulator_name": "ComReg, etc.",
      "evidence": ["Customer said: 'I'll report this to ComReg'", "call_date: 2026-02-15"]
    },
    "legal_threats": {
      "mentioned_legal_action": true | false,
      "legal_action_type": "solicitor | court action | breach of contract | legal threat",
      "evidence": ["Customer said: 'My solicitor will contact you'", "call_date: 2026-02-12"]
    },
    "cancellation_threats": {
      "threatened_cancellation": true | false,
      "cancellation_reason": "pricing | service_quality | unresolved_issue | competitor_offer | other",
      "evidence": ["Customer said: 'I'm closing my account'", "call_date: 2026-02-08"]
    },
    "escalation_threats": {
      "threatened_escalation": true | false,
      "escalation_target": "manager | CEO | legal | social media | regulator",
      "evidence": ["Customer said: 'Let me speak to your manager'", "call_date: 2026-02-10"]
      "B-016 FIX: Cancellation is NOT an escalation. If customer threatened cancellation, record it in cancellation_threats, NOT escalation_threats. Escalation = third-party threat (CEO, legal, regulator, social media). Cancellation = valid business decision to leave service."
    }
  },

  "retention_risk_signals": {
    "churn_probability": "Calculated by Python - leave blank",
    "risk_factors": [
      {
        "type": "competitor_comparison",
        "severity": "HIGH | MEDIUM | LOW",
        "evidence": "Customer quote mentioning competitor offer"
      },
      {
        "type": "regulatory_threat",
        "severity": "HIGH | MEDIUM | LOW",
        "evidence": "Customer mentioned ComReg"
      },
      {
        "type": "price_sensitivity",
        "severity": "MEDIUM",
        "evidence": "Customer compared our pricing to competitors"
      }
    ],
    "recommended_action": "Immediate retention outreach required | Monitor | Low priority"
  },

  "predictive_insights": {
    "next_contact_probability": "High | Medium | Low",
    "likely_contact_reason": "What they'll likely call about next",
    "churn_risk_indicator": "Elevated | Normal | Low based on data patterns",
    "optimal_contact_window": "Best time to reach out (if applicable)",
    "loyalty_signals": ["Positive indicators from their history"]
  },

  "value_at_risk": {
    "monthly_revenue": "€X.XX",
    "annual_revenue": "€X.XX",
    "revenue_segment": "High | Medium | Low Value",
    "revenue_breakdown": "€X from plans, €Y from devices, €Z from fixed services",
    "portfolio_details": "X SIMs, Y devices, Z services - describe bundle context",
    "products_at_risk": ["Mobile", "Fixed", "Device"],
    "churn_probability": "Calculated by Python - leave blank",
    "churn_risk_magnification": "Single SIM risk OR multi-SIM family/business bundle risk",
    "retention_priority": "Calculated by Python - leave blank"
  },

  "next_best_action": {
    "action": "Single most important action",
    "rationale": "Why this is the priority",
    "expected_outcome": "What we expect to achieve"
  },

  "escalation_risk": "Pending"  // Python will calculate this, leave as placeholder,

  "data_quality_warnings": [
    {
      "type": "CALL_RECORDINGS_NOT_ANALYZED",
      "severity": "HIGH | MEDIUM | LOW",
      "description": "Description of the data quality issue",
      "impact": "How this affects the analysis",
      "recommendation": "What should be done to fix",
      "data_source": "Table name where issue detected",
      "record_count": <number>
    }
  ]
}

CRITICAL REMINDERS:
1. EVERY recommendation must have specific evidence with IDs
2. Reference exact case IDs, interaction IDs, dates
3. Quote customer words VERBATIM when available - NEVER invent quotes
4. Show data source (table.field)
5. Explain WHY based on the data
6. Use "action_gating" to prevent inappropriate upsells
7. If customer threatened cancellation, DO NOT suggest adding services
8. If customer has unresolved issues, DO NOT suggest device upgrades
9. If customer complained about billing, DO NOT suggest higher plans
10. DO NOT calculate escalation_risk — leave it as "Pending" (Python calculates this post-LLM)
11. DO NOT specify churn_probability in retention_risk_signals or value_at_risk — Python calculates this from frustration_score to ensure consistency
12. State facts or say unknown - NO guessing

**ISSUE RESOLUTION ACTIONS - INCLUDE CONTEXT:**
When writing issue_resolution_actions, you MUST include:
✅ Case status (e.g., "Open for 14 days", "In Progress")
✅ Why it needs attention (e.g., "Customer called 3 times", "Affecting service")
✅ Specific impact (e.g., "Customer without mobile service since Feb 10")
✅ Expected outcome (e.g., "Restore service", "Resolve billing dispute")

GOOD EXAMPLE:
{
  "action": "Expedite ServiceNow case SN-265224 (SIM porting failure). Case is Open for 12 days; customer called 4 times (most recently 2 hours ago). Customer has been without mobile service since Feb 14. Contact Network Team immediately to expedite port and callback customer with ETA within 4 hours.",
  "evidence": {
    "case_id": "SN-265224",
    "case_status": "Open",
    "days_open": 12,
    "title": "SIM Porting Failure",
    "customer_contacts": 4,
    "impact": "Customer without mobile service for 12 days"
  },
  "priority": "CRITICAL",
  "owner": "Network Team",
  "expected_outcome": "SIM port completed, service restored, customer notified"
}

BAD EXAMPLE (too vague):
"Expedite resolution of ServiceNow case SN-265224 to address SIM porting failure."
❌ Missing: How long open? Why urgent? What's the impact? What's the expected outcome?

**CUSTOMER QUOTES - NO HALLUCINATIONS:**
✅ ALLOWED: Direct quotes from call_recordings.customer_quotes array
✅ ALLOWED: Paraphrasing with evidence: "Customer stated in call on 2026-02-20 that..."
❌ FORBIDDEN: Inventing quotes customer never said
❌ FORBIDDEN: Putting words in customer's mouth
❌ FORBIDDEN: Assuming customer said something without evidence

**PORTFOLIO CONTEXT - CRITICAL FOR RECOMMENDATIONS:**
When describing the customer and making recommendations, ALWAYS include:

1. **SIM Count Context:**
   - "Single SIM customer" vs "Customer with 3 SIMs (family/business account)"
   - If sim_count > 1: "Issue with one service could put entire bundle at risk"

2. **Device vs Plan Revenue:**
   - Break down: "€180/month = €90 from 3 mobile plans + €90 from 2 device installments"
   - Highlight risk: "2 devices ending in 6 months = €90/month revenue at risk"

3. **Mobile + Fixed Bundle:**
   - If customer_type = 'Mobile + Fixed': "Double revenue at risk (€X mobile + €Y fixed)"
   - Mention both services in all recommendations

4. **Churn Risk Magnification:**
   - Single SIM: "€35/month at risk"
   - Multi-SIM: "€180/month at risk across 3 SIMs - issue could trigger bundle cancellation"

EXAMPLE GOOD NARRATIVE:
"Customer holds 3 SIMs (€90/month from plans) and 2 device contracts (€90/month in installments), totaling €180/month. This appears to be a family/business account. Unresolved billing issue on primary SIM could jeopardize the entire bundle."

EXAMPLE GOOD RECOMMENDATION:
"Address billing query promptly. Customer has 3 SIMs paying €180/month total (€90 from plans, €90 from devices). This is a high-value family/business account where one unresolved issue could trigger churn of all 3 SIMs."

**THREAT DETECTION - EVIDENCE REQUIRED:**
✅ Customer mentioned "Three" or "Vodafone" → Include in threat_indicators.competitor_threats
✅ Customer said "I'll report to ComReg" → Include in threat_indicators.regulatory_threats
✅ Customer said "Let me speak to manager" → Include in threat_indicators.escalation_threats
✅ Customer said "I'm cancelling" or "closing my account" → Include in BOTH threat_indicators.cancellation_threats AND threat_indicators.escalation_threats.threatened_escalation = true
❌ FORBIDDEN: Assuming threats without evidence
❌ FORBIDDEN: "Customer might switch" → MUST have quote or evidence

**BUG FIX #17: PORTING DIRECTIONALITY - CRITICAL:**
❌ WRONG: Customer porting FROM Tesco TO Virgin Media → competitor_threats.mentioned_competitors=["Tesco Mobile"], switching_intent=True
✅ CORRECT: Customer porting FROM Tesco TO Virgin Media → switching_intent=False (this is an ACQUISITION, not a threat)
✅ CORRECT: Customer leaving Virgin Media for Three → competitor_threats with switching_intent=True

Porting Direction Rules:
- "porting from [competitor] to Virgin" OR "joining from [competitor]" → switching_intent=False (ACQUISITION - growth opportunity)
- "leaving Virgin for [competitor]" OR "switching to [competitor]" → switching_intent=True (CHURN THREAT)
- Default: If direction is unclear, check sentiment - positive about joining = False, negative about leaving = True

Examples:
❌ WRONG: "Customer is porting numbers from Tesco Mobile to Virgin Media" → competitor_threats with switching_intent=True
✅ CORRECT: "Customer is porting numbers from Tesco Mobile to Virgin Media" → switching_intent=False (acquisition, NOT a threat)
❌ WRONG: "Three offered me a better deal, I'm switching" → switching_intent=False
✅ CORRECT: "Three offered me a better deal, I'm switching" → switching_intent=True (churn threat)

**B7-H4 FIX: PURCHASE INTENT CLASSIFICATION - CRITICAL:**
❌ FORBIDDEN: Treating purchase intent as an unresolved issue that blocks upsell
✅ CORRECT: If customer wants to buy/upgrade/order → This is a SALES OPPORTUNITY, NOT a problem

Purchase Intent Classification Rules:
- issue_type = "order_status" → Customer wants to order something → MUST go to opportunity_actions[] (NOT key_issues[])
- issue_type = "plan_signup" → Customer wants to join/change plan → MUST go to opportunity_actions[] (NOT key_issues[])
- issue_type = "device_inquiry" → Customer wants device info/upgrade → MUST go to opportunity_actions[] (NOT key_issues[])
- If issue_type is ANY of these AND there's NO service failure → Classify as OPPORTUNITY, do NOT put in key_issues[]
- Only put in key_issues[] if there's an ACTUAL service failure (no service, billing error, technical issue)

Examples:
❌ WRONG: "I want to order a new phone" → key_issue with priority=HIGH, blocks upsell
✅ CORRECT: "I want to order a new phone" → opportunity_action: "Offer device upgrade options"

❌ WRONG: "Customer asking about family plan" → key_issue with priority=MEDIUM
✅ CORRECT: "Customer asking about family plan" → opportunity_action: "Present family plan options"

**RETENTION RISK SIGNALS:**
- HIGH SEVERITY: Customer compared pricing, mentioned competitor offers
- MEDIUM SEVERITY: Customer expressed dissatisfaction but no threats
- LOW SEVERITY: Customer asked questions but no negative sentiment

**AGENT BRIEFING - AGENT-FOCUSED QUICK REFERENCE:**
The agent_briefing field is designed for agents who need a quick, actionable overview. Think of this as the "cheat sheet" an agent would want to see before taking a call.

FORMAT REQUIREMENTS:
1. **who_are_they**: Single sentence profile
   - Example: "2 SIMs, no devices, €30/month"
   - Include: SIM count, device count, revenue - JUST THE FACTS
   - Do NOT add judgment labels like "frequent service issues" or "highly engaged" - this is only 30 days of data, not enough to establish patterns
   - If contact volume is notable, put it in whats_happened instead

2. **whats_happened**: ONE sentence core issue
   - Example: "Unresolved billing dispute for 14 days with 4 repeat contacts - customer threatening cancellation"
   - Focus on: What's the MAIN problem right now?
   - Include contact volume here if notable (e.g., "8 contacts in 30 days about...")

3. **what_to_do**: Clear action required
   - Example: "FIX billing issue FIRST - do NOT upsell until resolved"
   - Include: What MUST be done, what MUST be avoided

4. **human_briefing**: Natural language, human-readable briefing with emojis
   - Purpose: Give the team a quick, actionable overview at a glance
   - Format: Use emojis as visual indicators, structured sections
   - CRITICAL: MUST be grounded in actual data - specific case IDs, days, revenue, counts
   - NO HALLUCINATIONS - Only use facts from the data
   - Structure:
     * Risk indicators (escalation risk emoji, friction type emoji)
     * Customer profile (value level, revenue, services)
     * Current situation (what's happening, with specific case IDs and days)
     * Urgency/impact (why this matters)
   - DO NOT include recommended actions - those are in the recommended_actions section

   EXAMPLES:
   Low risk, no issues:
   "✅ Customer Stable: Low Friction
   Low-value customer (€20/month) with Mobile service. No active issues.
   Monitor routine inquiries."

   Medium risk, unresolved issue:
   "⚠️ Escalation Risk: Medium  ⚡ Friction: High-Value Unresolved
   Medium-value customer (€50/month) with Mobile service. Customer experiencing unresolved
   porting issues for 29 days (ServiceNow case INC0018985). Multiple contact attempts made.
   Immediate resolution required to prevent churn."

   High risk, legal threat:
   "🚨 Escalation Risk: HIGH  ⚡ Friction: Legal Threat + Unresolved Billing
   High-value customer (€975/month) with Mobile + Device services. Fraudulent activity reported
   plus unresolved billing dispute. Legal threat mentioned in recent call.
   CRITICAL - Immediate action required to prevent churn and legal escalation."

5. **data_sources_used**: List which data sources informed this briefing
   - Example: "Revenue_Cache, 8 Interactions, 2 Pega Cases, 1 ServiceNow Incident, Call Recordings"
   - This builds trust - agent knows what data was used

GOOD agent_briefing EXAMPLE:
{
  "who_are_they": "2 SIMs, no devices, €30/month",
  "whats_happened": "Service issues and billing confusion - 1 Pega case C-1028049 resolved on Feb 24, 2026 with all services restored",
  "what_to_do": "Verify services are working and billing is correct - monitor for any recurring issues",
  "human_briefing": "⚠️ Escalation Risk: Medium  ⚡ Friction: Recently Resolved
   Low-value customer (€30/month) with Mobile service. Service and billing issues were
   resolved on February 24, 2026 (Pega case C-1028049).
   Verify resolution is complete and monitor for any recurrence.",
  "data_sources_used": "Revenue_Cache, Interactions, Pega Cases"
}

**SOURCE SUMMARIES - INDIVIDUAL DATA SOURCE OVERVIEWS:**
The source_summaries field provides separate summaries for each data source. This restores the original v3-style summaries while maintaining v5's enhanced features.

FORMAT REQUIREMENTS:

1. **interactions_summary**: 1-2 sentences about contact patterns
   - Example: "Customer contacted 8 times in 30 days (6 calls, 2 webchats). Primary topics: SIM activation (5 contacts), billing (2 contacts), device inquiry (1 contact). High engagement pattern with 4 contacts in last 7 days indicating urgency."
   - Include: Total contacts, breakdown by type, key topics, frequency pattern

2. **pega_cases_summary**: Natural language summary of Pega cases with context
   - Purpose: Give team a clear understanding of case status and what needs attention
   - Use natural language with emojis as visual indicators (⚠️ for unresolved, ✅ for resolved)
   - MUST include: Specific case IDs, days open, status, what action is needed
   - Do NOT hallucinate - only use facts from the data
   - Example: "Customer has 2 Pega cases. ⚠️ 1 unresolved case (E-899062) has been Pending-AgentReview for 20 days since Feb 6, requiring immediate attention. ✅ 1 case resolved on Feb 10 (E-902040)."

3. **servicenow_summary**: Natural language summary of ServiceNow incidents with impact
   - Purpose: Explain what incidents affected the customer and their resolution status
   - Use natural language with context about customer impact
   - MUST include: Incident IDs, what happened, impact on customer, resolution status
   - Do NOT hallucinate - only use facts from the data
   - B7-M6 FIX: CRITICAL - Only generate ServiceNow narrative if data_availability.servicenow_cases=true
   - B7-M6 FIX: Do NOT mention ServiceNow case IDs from agent wrapups if cases not retrieved from ServiceNow API
   - B7-M6 FIX: If servicenow_cases array is empty, state: "No ServiceNow incidents retrieved for this customer."
   - Example: "Customer experienced 2 service incidents in February causing service suspension. Both incidents (INC0463883, INC0463295) were resolved on Feb 6, but the suspensions caused significant customer inconvenience and frustration."

4. **call_recordings_summary**: Natural language summary of call intelligence
   - Purpose: Surface what customers said in their own words, sentiment trends, and root causes
   - MUST include: Number of calls, specific customer quotes (verbatim), sentiment, threats mentioned
   - If no recordings: State this clearly and explain what analysis is based on
   - Do NOT fabricate quotes - only use actual customer words from transcripts
   - Example: "2 calls analyzed showing escalating frustration. Feb 15: 'This is ridiculous, I've been waiting 2 weeks' (negative). Feb 20: 'I'll contact my solicitor if not fixed' (negative, legal threat). Root cause: SIM provisioning failure."

GOOD source_summaries EXAMPLE:
{
  "interactions_summary": "Customer contacted 8 times in 30 days (6 calls, 2 webchats). Topics: SIM activation (5 contacts), billing (2 contacts), device inquiry (1 contact). High contact frequency with 4 contacts in last 7 days indicates escalating urgency about unresolved issues.",
  "pega_cases_summary": "Customer has 4 Pega cases. ⚠️ 1 unresolved case (E-899062) has been Pending-AgentReview for 20 days since February 6, 2026 - this is the primary driver of customer frustration. ✅ 3 cases resolved between Feb 9-10, 2026. Immediate attention needed on the pending case.",
  "servicenow_summary": "Customer experienced 2 ServiceNow incidents in February which caused service suspension. Both incidents (INC0463883, INC0463295) were resolved on February 6, 2026, but the suspensions resulted in 27 customer contacts about billing and service issues. Customer expressed significant frustration about the service disruption.",
  "call_recordings_summary": "No call recordings available. Analysis based on 27 interaction wrap-up comments shows customer expressed frustration about billing disputes and repeated service suspensions. Multiple escalation threats were noted in agent comments."
}
"""


# ============================================================
# EVIDENCE EXTRACTION & FRUSTRATION SCORING
# ============================================================

def clean_case_id(case_id):
    """Remove extra quotes from case_id fields.
    Database may store case_id as '"INC0473312"' instead of 'INC0473312'.
    """
    if not case_id or case_id == 'Unknown':
        return case_id

    # Convert to string and strip whitespace and quotes
    cleaned = str(case_id).strip()

    # Remove leading/trailing quotes
    if cleaned.startswith('"') and cleaned.endswith('"'):
        cleaned = cleaned[1:-1]
    elif cleaned.startswith("'") and cleaned.endswith("'"):
        cleaned = cleaned[1:-1]

    return cleaned


def extract_case_evidence(pega_cases, servicenow_cases):
    """Extract evidence from Pega and ServiceNow cases."""
    evidence = []
    now = datetime.now()

    # Process Pega cases
    for case in pega_cases or []:
        case_id = clean_case_id(case.get('case_id', 'Unknown'))
        status = case.get('status', 'Unknown')
        created = case.get('created_date')
        case_type = case.get('case_type', 'Unknown')

        if created:
            days_open = (now - created).days
        else:
            days_open = 0

        if status not in ['Resolved', 'Closed', 'Cancelled']:
            # CRITICAL FIX: Handle None case_type properly - omit title field when None/Unknown
            # instead of setting it to None which json.dumps with default=str converts to "None"
            evidence_item = {
                'type': 'unresolved_case',
                'source': 'Pega',
                'case_id': case_id,
                'status': status,
                'days_open': days_open,
                'reasoning': f"Pega case {case_id} ({case_type}) has been {status} for {days_open} days"
            }
            # Only include title field if we have a valid value
            if case_type and case_type != 'Unknown':
                evidence_item['title'] = case_type
            evidence.append(evidence_item)
        else:
            resolved_date = case.get('resolved_date')
            if resolved_date:
                days_since_resolution = (now - resolved_date).days
                if days_since_resolution <= 3:
                    # CRITICAL FIX: Handle None case_type properly - omit title field when None/Unknown
                    evidence_item = {
                        'type': 'recently_resolved_case',
                        'source': 'Pega',
                        'case_id': case_id,
                        'status': status,
                        'days_since_resolution': days_since_resolution,
                        'reasoning': f"Pega case {case_id} was resolved {days_since_resolution} days ago. Verify customer satisfaction."
                    }
                    # Only include title field if we have a valid value
                    if case_type and case_type != 'Unknown':
                        evidence_item['title'] = case_type
                    evidence.append(evidence_item)

    # Process ServiceNow cases
    for case in servicenow_cases or []:
        incident_number = clean_case_id(case.get('incident_number', 'Unknown'))
        status = case.get('status', 'Unknown')
        created = case.get('created_date')
        title = case.get('title', 'Unknown')

        if created:
            days_open = (now - created).days
        else:
            days_open = 0

        if status not in ['Resolved', 'Closed', 'Cancelled']:
            evidence.append({
                'type': 'unresolved_case',
                'source': 'ServiceNow',
                'case_id': incident_number,
                'title': title,
                'status': status,
                'days_open': days_open,
                'reasoning': f"ServiceNow case {incident_number} ('{title}') has been {status} for {days_open} days"
            })
        else:
            resolved_date = case.get('resolved_date')
            if resolved_date:
                days_since_resolution = (now - resolved_date).days
                if days_since_resolution <= 3:
                    evidence.append({
                        'type': 'recently_resolved_case',
                        'source': 'ServiceNow',
                        'case_id': incident_number,
                        'title': title,
                        'status': status,
                        'days_since_resolution': days_since_resolution,
                        'reasoning': f"ServiceNow case {incident_number} was resolved {days_since_resolution} days ago. Verify satisfaction."
                    })

    return evidence


def extract_call_recording_evidence(call_recordings):
    """Extract evidence from call transcript issues.

    CRITICAL: Call recordings contain UNRESOLVED issues that must be counted
    in frustration_score, just like Pega/ServiceNow cases.
    """
    evidence = []
    now = datetime.now()

    for recording in call_recordings or []:
        call_id = recording.get('recording_id', 'Unknown')
        call_date = recording.get('call_date')
        call_issues = recording.get('call_issues', [])

        # Calculate days since call
        if call_date:
            try:
                if isinstance(call_date, str):
                    call_dt = datetime.fromisoformat(call_date.replace('T', ' ').replace('Z', ''))
                else:
                    call_dt = call_date
                days_since_call = (now - call_dt).days
            except Exception:
                days_since_call = 0
        else:
            days_since_call = 0

        # Process unresolved issues from call recordings
        for issue in call_issues or []:
            if isinstance(issue, dict):
                resolution_status = issue.get('resolution_status', 'UNKNOWN')
                reason_display = issue.get('reason_display', 'Unknown issue')
                reason_key = issue.get('reason_key', 'unknown')
                calibrated_band = issue.get('calibrated_band', 'UNKNOWN')

                # Only count UNRESOLVED issues
                if resolution_status == 'UNRESOLVED':
                    # Use days_since_call as days_open (issue has been open since the call)
                    days_open = days_since_call

                    # Weight by severity (calibrated_band)
                    if calibrated_band == 'CRITICAL':
                        score_weight = 50
                    elif calibrated_band == 'HIGH':
                        score_weight = 35
                    elif calibrated_band == 'MEDIUM':
                        score_weight = 20
                    else:
                        score_weight = 10

                    evidence.append({
                        'type': 'unresolved_call_issue',
                        'source': 'CallRecording',
                        'call_id': call_id,
                        'issue_type': reason_key,
                        'title': reason_display,
                        'severity': calibrated_band,
                        'status': resolution_status,
                        'days_open': days_open,
                        'score_weight': score_weight,
                        'reasoning': f"Call {call_id} identified UNRESOLVED {reason_display} ({calibrated_band} severity) from {days_open} days ago"
                    })

    return evidence


def extract_interaction_evidence(interactions):
    """Extract evidence from customer interactions."""
    evidence = []
    cancellation_threats = []
    negative_keywords = ['cancel', 'refund', 'manager', 'overcharge', 'error', 'wrong', 'terrible', 'unacceptable']
    positive_keywords = ['upgrade', 'interest', 'plan', 'offer', 'new']

    # Keywords and patterns indicating unresolved issues from agent wrapup comments
    unresolved_issue_patterns = [
        ('unable to', 20), ('cannot', 20), ('can\'t', 20), ('blocked', 25),
        ('failed', 15), ('failure', 15), ('not working', 15),
        ('not received', 20), ('didn\'t receive', 20), ('never received', 25),
        ('never arrived', 25), ('wrong address', 20), ('incorrect address', 20),
        ('address change', 15), ('sim not received', 25), ('sim not working', 20),
        ('no service', 15), ('error', 10), ('issue', 5), ('problem', 5)
    ]

    # Check for unresolved issues in wrapup comments
    # CRITICAL: Count EACH issue separately if multiple issues mentioned
    unresolved_wrapup_evidence = []
    for interaction in interactions or []:
        interaction_id = interaction.get('interaction_id', 'Unknown')
        wrapup = str(interaction.get('agent_wrapup_comment', '')).lower()
        interaction_date = interaction.get('interaction_date')

        if wrapup:
            # Count how many unresolved issue patterns match
            issue_count = 0
            matched_patterns = []
            total_score = 0
            for pattern, score_weight in unresolved_issue_patterns:
                if pattern in wrapup:
                    issue_count += 1
                    matched_patterns.append(pattern)
                    total_score += score_weight

            if issue_count > 0:
                unresolved_wrapup_evidence.append({
                    'interaction_id': interaction_id,
                    'date': str(interaction_date) if interaction_date else 'Unknown',
                    'wrapup_comment': wrapup[:150],
                    'issue_count': issue_count,
                    'total_score': total_score,
                    'reasoning': f"Agent noted {issue_count} unresolved issue(s) in wrapup: {wrapup[:100]}..."
                })

    # Check for cancellation threats
    cancellation_threats = []
    for interaction in interactions or []:
        interaction_id = interaction.get('interaction_id', 'Unknown')
        wrapup = str(interaction.get('agent_wrapup_comment', '')).lower()
        interaction_date = interaction.get('interaction_date')

        # Detect cancellation threats
        if wrapup and 'cancel' in wrapup:
            # Extract quote if possible
            quote = extract_customer_quote(wrapup)
            cancellation_threats.append({
                'interaction_id': interaction_id,
                'date': str(interaction_date) if interaction_date else 'Unknown',
                'quote': quote,
                'reasoning': f"Customer mentioned cancellation in interaction {interaction_id} on {interaction_date}"
            })

    # Add unresolved wrapup issues to evidence
    if unresolved_wrapup_evidence:
        for issue in unresolved_wrapup_evidence:
            evidence.append({
                'type': 'unresolved_issue_from_wrapup',
                'source': 'Interaction_wrapup',
                'interaction_id': issue['interaction_id'],
                'date': issue['date'],
                'wrapup_comment': issue['wrapup_comment'],
                'issue_count': issue.get('issue_count', 1),
                'total_score': issue.get('total_score', 25),
                'reasoning': issue['reasoning']
            })

    if cancellation_threats:
        for threat in cancellation_threats:
            evidence.append({
                'type': 'cancellation_threat',
                'source': 'Interaction',
                'interaction_id': threat['interaction_id'],
                'date': threat['date'],
                'quote': threat['quote'],
                'reasoning': threat['reasoning']
            })

    # Check for escalation threats - CEO/executive complaints, manager demands, regulatory threats
    escalation_threats = []
    escalation_keywords = [
        'ceo', 'chief executive', 'executive office', 'executive team',
        'managing director', 'md', 'chairman', 'board',
        'comreg', 'commission', 'regulator', 'comreg complaint',
        'legal', 'solicitor', 'lawyer', 'court', 'tribunal',
        'social media', 'twitter', 'facebook', 'linkedin', 'media', 'press', 'journalist'
    ]

    # B7-H1 FIX: Exclusion patterns to prevent false positives
    # 1. Company self-references (Virgin Media, VM, VMI, Virgin)
    # 2. Address patterns (Court, Drive, Avenue, Close in Irish addresses)
    # 3. Case category references ("working ceo complaints case")
    company_exclusion_patterns = [
        r'\bvirgin media\b',
        r'\bvm\s',
        r'\bvmi\b',
        r'\bvirgin\s+customer\b',
        r'\bvirgin\s+mobile\b'
    ]

    # B7-H1 FIX: Address pattern to exclude - Irish addresses have common street suffixes
    # Pattern: [number] [street_name] [Court/Drive/Avenue/Close/Road/Street]
    address_pattern = re.compile(r'\b\d+\s+[\w\s]+?\s+(court|drive|avenue|close|road|street|lane|place|square)\b', re.IGNORECASE)

    # B7-H2 FIX: Case category reference pattern - "working ceo complaints case [ID]"
    # This is NOT a customer threat, it's an agent describing their work
    case_category_pattern = re.compile(r'\bworking\s+(ceo\s+complaints|regulatory|legal|comreg)\s+case\b', re.IGNORECASE)

    for interaction in interactions or []:
        interaction_id = interaction.get('interaction_id', 'Unknown')
        wrapup_raw = interaction.get('agent_wrapup_comment', '')
        wrapup = str(wrapup_raw).lower() if wrapup_raw else ''
        interaction_date = interaction.get('interaction_date')

        if wrapup:
            # B7-H2 FIX: Skip if this is a case category reference (agent workflow, not customer threat)
            if case_category_pattern.search(wrapup):
                logger.info(f"  B7-H2 FIX: Skipping escalation detection - case category reference in interaction {interaction_id}: '{wrapup[:100]}...'")
                continue

            # B7-H1 FIX: Remove company self-references before keyword matching
            clean_wrapup = wrapup
            for pattern in company_exclusion_patterns:
                clean_wrapup = re.sub(pattern, ' ', clean_wrapup, flags=re.IGNORECASE)

            # B7-H1 FIX: Remove address patterns before keyword matching
            # Split by address pattern and only keep non-address parts for escalation detection
            wrapup_parts = address_pattern.split(clean_wrapup)
            clean_wrapup = ' '.join(wrapup_parts)

            # Check for escalation keywords in cleaned text
            matched_escalations = []
            escalation_target = None

            for keyword in escalation_keywords:
                if keyword in clean_wrapup:
                    # B7-H1 FIX: Additional check - keyword must not be part of company name or address
                    # E.g., 'media' only counts if NOT in 'virgin media'
                    # E.g., 'court' only counts if NOT in 'killiney court'
                    if keyword == 'media':
                        # Check if 'media' appears standalone, not in 'virgin media'
                        if 'virgin media' in wrapup or 'virgin' in clean_wrapup.split()[:5]:
                            logger.info(f"  B7-H1 FIX: Skipping 'media' keyword - part of 'Virgin Media' company reference in interaction {interaction_id}")
                            continue
                    if keyword == 'court':
                        # Check if 'court' is part of address (has number before it)
                        if address_pattern.search(wrapup):
                            logger.info(f"  B7-H1 FIX: Skipping 'court' keyword - part of address pattern in interaction {interaction_id}")
                            continue

                    matched_escalations.append(keyword)

                    # Determine escalation target
                    if keyword in ['ceo', 'chief executive', 'executive office', 'executive team', 'managing director', 'md', 'chairman', 'board']:
                        escalation_target = 'CEO'
                    elif keyword in ['comreg', 'commission', 'regulator', 'comreg complaint']:
                        escalation_target = 'regulatory'
                    elif keyword in ['legal', 'solicitor', 'lawyer', 'court', 'tribunal']:
                        escalation_target = 'legal'
                    elif keyword in ['social media', 'twitter', 'facebook', 'linkedin', 'media', 'press', 'journalist']:
                        escalation_target = 'social_media'

            if matched_escalations:
                # B7-H3 FIX: Use actual wrapup text, not system-generated message
                # Truncate to 200 chars if needed
                quote_text = wrapup_raw[:200] if wrapup_raw else clean_wrapup[:200]

                escalation_threats.append({
                    'interaction_id': interaction_id,
                    'date': str(interaction_date) if interaction_date else 'Unknown',
                    'quote': quote_text,  # B7-H3 FIX: Actual wrapup text, not "Escalation detected: X"
                    'escalation_target': escalation_target or 'manager',
                    'matched_keywords': matched_escalations,
                    'reasoning': f"Escalation threat detected in wrapup: {', '.join(matched_escalations[:3])}"
                })

    # Add escalation threats to evidence
    if escalation_threats:
        for threat in escalation_threats:
            evidence.append({
                'type': 'escalation_threat',
                'source': 'Interaction_wrapup',
                'interaction_id': threat['interaction_id'],
                'date': threat['date'],
                'quote': threat['quote'],
                'escalation_target': threat['escalation_target'],
                'matched_keywords': threat['matched_keywords'],
                'reasoning': threat['reasoning']
            })

    # Check for repeat contacts - NEW: Distinguish between frustration and engagement
    # Note: We'll check for unresolved cases in the scoring phase, not here
    if interactions and len(interactions) >= 4:
        # For now, just record the contact count - severity will be calculated based on cases
        if len(interactions) >= 6:
            evidence.append({
                'type': 'repeat_contacts',
                'source': 'Interaction',
                'count': len(interactions),
                'reasoning': f"Customer has contacted us {len(interactions)} times in 30 days - indicates either high engagement or unresolved issues"
            })
        elif len(interactions) >= 4:
            evidence.append({
                'type': 'repeat_contacts',
                'source': 'Interaction',
                'count': len(interactions),
                'reasoning': f"Customer has contacted us {len(interactions)} times in 30 days - monitor for pattern"
            })

    return evidence


def is_agent_paraphrase(text):
    """Detect if text is an agent paraphrase rather than a verbatim customer quote.
    Agent paraphrases use third-person language like "Customer wants to...".
    Verbatim quotes use first-person language like "I want to...".
    """
    text_lower = text.lower().strip()

    # Patterns that indicate agent paraphrase (third-person)
    paraphrase_patterns = [
        'customer wants to',
        'customer would like',
        'customer is asking for',
        'customer needs',
        'customer requests',
        'customer is requesting',
        'customer wishes',
        'customer looking for',
        'customer enquiring about',
        'customer inquiring about',
        'customer states they want',
        'customer stated they want',
        'customer mentioned they want',
        'wants to waive',
        'would like to waive',
        'asking for waiver',
        'requesting waiver',
    ]

    for pattern in paraphrase_patterns:
        if pattern in text_lower:
            return True

    return False


def extract_customer_quote(text):
    """Extract customer quote from text.
    Filter out agent paraphrases - only return verbatim customer speech.
    """
    # Look for quoted text
    quote_patterns = [
        r'"([^"]+)"',
        r"'([^']+)'",
        r'customer said: (.+)',
        r'stated: (.+)',
    ]

    for pattern in quote_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            potential_quote = match.group(1).strip()
            # Filter out agent paraphrases
            if not is_agent_paraphrase(potential_quote):
                return potential_quote

    # If no quote found, return empty string (not a paraphrase summary)
    if 'cancel' in text.lower():
        return "Mentioned cancellation"
    return ""


def extract_customer_quotes_and_threats(call_recordings):
    """
    Extract customer quotes and detect threats from call recordings.
    Returns list of quote objects with threat classification.

    CRITICAL: Only include ACTUAL quotes from call_recordings.customer_quotes
    NEVER invent or hallucinate customer quotes.
    """

    quotes = []
    competitors_found = set()
    comreg_found = False
    escalation_found = False

    for recording in call_recordings or []:
        call_id = recording.get('recording_id')
        call_date = recording.get('call_date')
        customer_quotes_array = recording.get('customer_quotes', [])

        if customer_quotes_array and isinstance(customer_quotes_array, list):
            for quote_obj in customer_quotes_array:
                # Extract actual quote text
                quote_text = quote_obj.get('text', quote_obj.get('quote', ''))

                if not quote_text:
                    continue

                # Classify threat type
                threat_type = "none"
                threat_details = {}

                quote_lower = quote_text.lower()

                # Check for competitor mentions - DIRECTION AWARE
                # KEY: Distinguish "switch from Three" (GOOD - joining us) vs "switch to Three" (BAD - leaving us)

                # First, check if this is a POSITIVE mention (customer joining us FROM competitor)
                # Patterns like "switch from Three", "coming from Vodafone", "left Eir"
                positive_joining_patterns = [
                    r'\b(switch|switched|moving|moved|coming|came|leaving|left|churning|churned)\s+\bfrom\b\s*(three|3|vodafone|eir|air|aer|ear|voda|lyca)',
                    r'\b(joining|joined)\s+(you|virgin)\s+(from|after)\s*(three|3|vodafone|eir|air)',
                    r'\b(three|3|vodafone|eir|air|aer|ear)\s+(was|were|is)\s+(terrible|awful|horrible|bad-useless|crap)',
                ]

                is_joining_us = False
                for pattern in positive_joining_patterns:
                    if re.search(pattern, quote_lower):
                        is_joining_us = True  # Customer is leaving competitor to join us - NOT a threat!
                        break

                # Phonetic variations for common transcription errors
                # Eir is often transcribed as Air, Aer, Ear due to Irish pronunciation
                competitor_phonetic_map = {
                    'Three': [r'\bthree\b', r'\b3\b'],
                    'Vodafone': [r'\bvodafone\b', r'\bvoda\b'],
                    'Eir': [r'\beir\b', r'\bair\b(?!\s+time)', r'\baer\b', r'\bear\b(?!\s+time|\bly\b)'],  # "Air" not "airtime"
                    'Tesco Mobile': [r'\btesco\s+mobile\b'],
                    'Lyca': [r'\blyca\b'],
                    'Sky Mobile': [r'\bsky\s+mobile\b'],
                }

                # Check for competitor mentions with context
                competitor_context_patterns = {
                    # Three - require network context
                    'Three': r'\bthree\s+(network|mobile|broadband|plan|deal|offer|store|shop|coverage|signal|sim|bill|contract)',
                    # Vodafone - unique name
                    'Vodafone': r'\bvodafone',
                    # Eir - with phonetic variations (Air, Aer, Ear)
                    'Eir': r'\beir\s+(mobile|broadband|plan|deal)|\bair\s+mobile|\baer\s+mobile',
                    # Others
                    'Tesco Mobile': r'\btesco\s+mobile',
                    'Lyca': r'\blyca\s*(mobile)?',
                    'Sky Mobile': r'\bsky\s+mobile',
                }

                competitor_mentioned = None
                for competitor, pattern in competitor_context_patterns.items():
                    if re.search(pattern, quote_lower):
                        competitor_mentioned = competitor
                        break

                # Determine if this is a threat based on DIRECTION and CONTEXT
                if competitor_mentioned and not is_joining_us:
                    # Look for threat indicators (switching AWAY from us)
                    threat_indicators = [
                        # Switching AWAY from us (to competitor)
                        r'\b(switch|switching|move|moving|leave|leaving|go|going|change|changing)\s+\bto\b\s*(three|3|vodafone|eir|air|aer|lyca)',
                        # Competitor has better offering (including "Air mobile")
                        r'\b(three|3|vodafone|eir|air|aer)\s*(mobile|network|broadband)?\b.*\b(better|cheaper|offer|deal|discount|best)\b',
                        # Considering competitors
                        r'\b(think|thinking|consider|considering|look|looking)\s+(at|to|for)\s*(three|3|vodafone|eir|air|lyca)\s*(mobile|network|broadband)?',
                        # Comparison favoring competitor
                        r'\b(three|3|vodafone|eir|air|aer|lyca)\s*(mobile|network)?\b.*\b(better|cheaper|faster|more\s+reliable|less\s+expensive)',
                    ]

                    for pattern in threat_indicators:
                        if re.search(pattern, quote_lower):
                            threat_type = "competitor_threat"
                            threat_details['competitor'] = competitor_mentioned
                            competitors_found.add(competitor_mentioned)
                            break

                # Check for regulatory threats (ComReg)
                if re.search(r'\bcomreg\b', quote_lower):
                    threat_type = "regulatory_threat"
                    threat_details['regulator'] = "ComReg"
                    comreg_found = True

                # Check for other regulatory mentions
                if re.search(r'\bregulator\b|\bcommission\b|\bombudsman\b', quote_lower):
                    threat_type = "regulatory_threat"
                    threat_details['regulator'] = "Regulatory body"

                # Check for LEGAL threats (CRITICAL - customer prepared to take legal action)
                legal_threat_patterns = [
                    # Legal representation
                    r'\b(solicitor|lawyer|legal\s+(advice|action|representation|team|counsel))',
                    # Court action
                    r'\b(suing|sue|small\s+claims\s+court|court\s+action|court\s+case|going\s+to\s+court)',
                    # Legal threats
                    r'\b(legal\s+(action|proceedings|steps|matter)|take\s+legal\s+action|legal\s+notice)',
                    # Consumer rights (often precedes legal action)
                    r'\b(consumer\s+rights|citizens?\s+information|consumer\s+agency|department\s+of\s+enterprise)',
                    # Contract breach language
                    r'\b(breach\s+of\s+contract|contractual\s+obligation|legal\s+obligation)',
                ]
                for pattern in legal_threat_patterns:
                    if re.search(pattern, quote_lower):
                        threat_type = "legal_threat"
                        # Determine what type of legal action
                        # FIX: Added parentheses for correct operator precedence
                        if re.search(r'\b(solicitor|lawyer)\b', quote_lower):
                            threat_details['legal_action'] = 'Legal representation'
                        elif re.search(r'\b(suing|sue|court)\b', quote_lower):
                            threat_details['legal_action'] = 'Court action'
                        elif re.search(r'\bbreach\s+of\s+contract', quote_lower):
                            threat_details['legal_action'] = 'Contract breach claim'
                        else:
                            threat_details['legal_action'] = 'Legal threat'
                        break

                # Check for cancellation threats
                cancellation_patterns = [
                    r'\bcancel\b.*\b(account|service|subscription|contract|mobile|phone)\b',
                    r'\bclose\b.*\baccount\b',
                    r'\bleave\b.*\bvirgin\b',
                    r'\bterminate\b.*\bservice\b',
                    r'\bdisconnect\b.*\bservice\b',
                ]
                for pattern in cancellation_patterns:
                    if re.search(pattern, quote_lower):
                        threat_type = "cancellation_threat"
                        break

                # Check for escalation threats
                escalation_patterns = [
                    r'\bspeak\s+to\s+(your\s+)?manager',
                    r'\bmanager\b.*\bcall\b|\bcall\b.*\bmanager\b',
                    r'\bescalate\b',  # FIX: Removed typo '\l'
                    r'\bceo\b',
                    r'\blegal\b.*\bteam\b',
                    r'\bformal\s+complaint',
                ]
                for pattern in escalation_patterns:
                    if re.search(pattern, quote_lower):
                        if threat_type == "none":
                            threat_type = "escalation_threat"
                        if 'manager' in quote_lower:
                            threat_details['escalation_target'] = 'manager'
                        break

                # Only add if there's meaningful content
                if len(quote_text.strip()) > 5:
                    quotes.append({
                        'quote': quote_text,
                        'call_date': str(call_date) if call_date else None,
                        'call_id': call_id,
                        'threat_type': threat_type,
                        **threat_details
                    })

    return {
        'quotes': quotes,
        'competitors_found': list(competitors_found),
        'comreg_mentioned': comreg_found,
        'escalation_mentioned': escalation_found
    }


def extract_threat_evidence_from_quotes(customer_quotes_analysis):
    """
    Convert customer quotes analysis into threat evidence for frustration scoring.

    CRITICAL: Only creates evidence for quotes with actual threat indicators detected.
    NO HALLUCINATIONS - every evidence item must have an actual quote backing it.
    """
    evidence = []

    if not customer_quotes_analysis:
        return evidence

    quotes = customer_quotes_analysis.get('quotes', [])

    for quote_item in quotes:
        threat_type = quote_item.get('threat_type', 'none')

        # Only create evidence for actual threats
        if threat_type == 'competitor_threat':
            competitor = quote_item.get('competitor', 'unknown competitor')
            evidence.append({
                'type': 'competitor_threat',
                'source': 'Call Recording',
                'call_id': quote_item.get('call_id'),
                'date': quote_item.get('call_date'),
                'quote': quote_item.get('quote'),
                'competitor': competitor,
                'reasoning': f"Customer mentioned competitor '{competitor}' in call recording: '{quote_item.get('quote', '')[:100]}...'"
            })
        elif threat_type == 'regulatory_threat':
            regulator = quote_item.get('regulator', 'regulatory body')
            evidence.append({
                'type': 'regulatory_threat',
                'source': 'Call Recording',
                'call_id': quote_item.get('call_id'),
                'date': quote_item.get('call_date'),
                'quote': quote_item.get('quote'),
                'regulator': regulator,
                'reasoning': f"Customer mentioned escalation to {regulator} in call recording: '{quote_item.get('quote', '')[:100]}...'"
            })
        elif threat_type == 'legal_threat':
            legal_action = quote_item.get('legal_action', 'Legal threat')
            evidence.append({
                'type': 'legal_threat',
                'source': 'Call Recording',
                'call_id': quote_item.get('call_id'),
                'date': quote_item.get('call_date'),
                'quote': quote_item.get('quote'),
                'legal_action': legal_action,
                'reasoning': f"Customer threatened {legal_action.lower()} in call recording: '{quote_item.get('quote', '')[:100]}...'"
            })
        elif threat_type == 'escalation_threat':
            escalation_target = quote_item.get('escalation_target', 'escalation')
            evidence.append({
                'type': 'escalation_threat',
                'source': 'Call Recording',
                'call_id': quote_item.get('call_id'),
                'date': quote_item.get('call_date'),
                'quote': quote_item.get('quote'),
                'escalation_target': escalation_target,
                'reasoning': f"Customer demanded {escalation_target} in call recording: '{quote_item.get('quote', '')[:100]}...'"
            })
        elif threat_type == 'cancellation_threat':
            evidence.append({
                'type': 'cancellation_threat',
                'source': 'Call Recording',
                'call_id': quote_item.get('call_id'),
                'date': quote_item.get('call_date'),
                'quote': quote_item.get('quote'),
                'reasoning': f"Customer threatened cancellation in call recording: '{quote_item.get('quote', '')[:100]}...'"
            })

    return evidence


def extract_revenue_opportunities(devices, profile):
    """Extract upsell opportunities from Customer_Device_Assets (primary) and Revenue_Cache (fallback).

    Priority: Customer_Device_Assets (actual contract data) > Revenue_Cache (calculated).
    """
    opportunities = []

    # ============================================================
    # DEVICE UPGRADE OPPORTUNITY
    # ============================================================

    # PRIMARY: Use Customer_Device_Assets if available (most accurate)
    if devices:
        for device in devices:
            if not device.get('is_contract_active'):
                continue

            contract_end = device.get('contract_end_date')
            if contract_end:
                try:
                    end_date = datetime.strptime(contract_end[:10], '%Y-%m-%d')
                    days_remaining = (end_date - datetime.now()).days

                    # Opportunity: Contract ending within 6 months (180 days)
                    if days_remaining <= 180 and days_remaining > 0:
                        monthly_installment = device.get('monthly_installment', 0)

                        opportunities.append({
                            'type': 'device_upgrade_opportunity',
                            'source': 'Customer_Device_Assets',
                            'device_brand': device.get('device_brand'),
                            'device_model': device.get('device_model'),
                            'contract_end_date': contract_end[:10],
                            'days_remaining': days_remaining,
                            'monthly_installment': monthly_installment,
                            'reasoning': f"Device contract {device.get('device_brand')} {device.get('device_model')} ends in {days_remaining} days ({contract_end[:10]}). Monthly installment: €{monthly_installment:.2f}. Optimal upgrade timing.",
                            'data_source': 'Customer_Device_Assets.contract_end_date (actual contract data)'
                        })
                except Exception as e:
                    logger.warning(f"Error parsing device contract end date: {e}")

    # NOTE: Device upgrade opportunities now come ONLY from Customer_Device_Assets
    # The following columns were dropped from Revenue_Cache and are no longer available:
    # - handset_remaining_installments, handset_monthly_installment, handset_original_cost
    # - device_cost_total, upfront_device_cost
    # All device details must be fetched from Customer_Device_Assets table

    # Family plan opportunity
    plan_count = profile.get('plan_count') or 0  # Number of active plans
    if plan_count >= 3:
        monthly_revenue = profile.get('monthly_revenue_total', 0)
        opportunities.append({
            'type': 'family_plan_opportunity',
            'plan_count': plan_count,  # BUG FIX: Use plan_count key for clarity
            'monthly_revenue': monthly_revenue,
            'reasoning': f"{plan_count} plans/SIMs on account with €{monthly_revenue:.2f}/month revenue. Family plan could save customer money.",
            'data_source': 'Revenue_Cache.plan_count'
        })

    # Prepaid conversion opportunity
    service_type = profile.get('service_type')
    monthly_plan = profile.get('monthly_revenue_plan', 0)
    if service_type == 'Prepaid' and monthly_plan > 50:
        opportunities.append({
            'type': 'postpaid_conversion_opportunity',
            'current_revenue': monthly_plan,
            'reasoning': f"Prepaid customer spending €{monthly_plan:.2f}/month consistently. Convert to postpaid for contract lock-in.",
            'data_source': 'Revenue_Cache.service_type, monthly_revenue_plan'
        })

    return opportunities


def calculate_frustration_score(evidence_list):
    """
    Calculate deterministic frustration score (0-100).
    Higher score = more frustrated = NO UPSELL allowed.

    ENHANCED: Distinguish between high engagement and actual frustration.
    """
    score = 0

    # First pass: Count unresolved cases (from Pega, ServiceNow, call recordings, AND wrapups)
    has_unresolved_cases = any(
        e.get('type') in ['unresolved_case', 'unresolved_call_issue', 'unresolved_issue_from_wrapup']
        for e in evidence_list
    )

    for evidence in evidence_list:
        e_type = evidence.get('type')

        # Unresolved cases from Pega/ServiceNow (major frustration)
        if e_type == 'unresolved_case':
            days_open = evidence.get('days_open', 0)
            if days_open > 14:
                score += 40
            elif days_open > 7:
                score += 25
            else:
                score += 10

        # CRITICAL FIX: Unresolved issues from call recordings
        elif e_type == 'unresolved_call_issue':
            days_open = evidence.get('days_open', 0)
            score_weight = evidence.get('score_weight', 10)

            # Base score from severity weighting
            score += score_weight

            # Additional penalty for long-standing unresolved issues
            if days_open > 14:
                score += 15
            elif days_open > 7:
                score += 10

        # CRITICAL FIX: Unresolved issues from agent wrapup comments
        elif e_type == 'unresolved_issue_from_wrapup':
            # These are issues agents couldn't resolve - HIGH frustration indicator
            # Use the calculated total_score which accounts for multiple issues
            total_score = evidence.get('total_score', 25)
            issue_count = evidence.get('issue_count', 1)

            # Base score from detected issues (accounts for multiple issues in one wrapup)
            score += total_score

            # Additional penalty if multiple issues in same wrapup
            if issue_count >= 2:
                score += 15  # Bonus frustration for compound problems

        # Cancellation threats (critical!)
        elif e_type == 'cancellation_threat':
            score += 35

        # Repeat contacts - ENHANCED: Check if cases exist
        elif e_type == 'repeat_contacts':
            count = evidence.get('count', 0)
            if count >= 6:
                if has_unresolved_cases:
                    # HIGH frustration: 6+ contacts WITH unresolved cases
                    # This is frustrated repeat calling about the same problem
                    score += 35
                else:
                    # LOW frustration: 6+ contacts WITHOUT cases
                    # This is likely high engagement, not frustration
                    score += 10
            elif count >= 4:
                if has_unresolved_cases:
                    # MEDIUM frustration: 4-5 contacts with cases
                    score += 20
                else:
                    # LOW frustration: 4-5 contacts without cases
                    score += 8

        # Recently resolved (caution)
        elif e_type == 'recently_resolved_case':
            score += 10

        # NEW: Competitor threats (HIGH churn risk!)
        elif e_type == 'competitor_threat':
            score += 50  # CRITICAL: Customer comparing to competitors

        # NEW: Legal threats (CRITICAL - customer prepared to take legal action!)
        elif e_type == 'legal_threat':
            score += 55  # CRITICAL: Solicitor/court action = immediate attention required

        # NEW: Regulatory threats (CRITICAL - legal risk!)
        elif e_type == 'regulatory_threat':
            score += 60  # CRITICAL: ComReg mention = highest priority

        # NEW: Escalation threats (HIGH frustration)
        elif e_type == 'escalation_threat':
            score += 40  # VERY HIGH: Demanding escalation

    return min(score, 100)  # Cap at 100


def determine_gating_decision(frustration_score, evidence_list):
    """
    Determine if upsell is safe based on frustration score.
    Returns gating dict with specific evidence.
    """
    # B7-H6 FIX: Calculate frustration label for accurate reason text
    if frustration_score >= 75:
        frustration_label = 'CRITICAL'
    elif frustration_score >= 60:
        frustration_label = 'HIGH'
    elif frustration_score >= 40:
        frustration_label = 'MEDIUM'
    elif frustration_score >= 20:
        frustration_label = 'LOW'
    else:
        frustration_label = 'VERY LOW'

    if frustration_score >= FRUSTRATION_THRESHOLD_HIGH:
        # HIGH frustration - NO UPSELL
        unresolved_cases = [e for e in evidence_list if e.get('type') == 'unresolved_case']
        cancellation_threats = [e for e in evidence_list if e.get('type') == 'cancellation_threat']

        # NEW: Threat evidence from customer quotes
        competitor_threats = [e for e in evidence_list if e.get('type') == 'competitor_threat']
        regulatory_threats = [e for e in evidence_list if e.get('type') == 'regulatory_threat']
        legal_threats = [e for e in evidence_list if e.get('type') == 'legal_threat']
        escalation_threats = [e for e in evidence_list if e.get('type') == 'escalation_threat']

        reason_parts = []

        # Regulatory threats (HIGHEST priority - ComReg)
        if regulatory_threats:
            threat = regulatory_threats[0]
            regulator = threat.get('regulator', 'regulator')
            quote = threat.get('quote', '')[:80]
            reason_parts.append(f"Customer mentioned escalation to {regulator}: '{quote}...'")

        # Legal threats (CRITICAL - solicitor/court action)
        if legal_threats:
            threat = legal_threats[0]
            legal_action = threat.get('legal_action', 'legal action')
            quote = threat.get('quote', '')[:80]
            reason_parts.append(f"Customer threatened {legal_action.lower()}: '{quote}...'")

        # Competitor threats (HIGH churn risk)
        if competitor_threats:
            threat = competitor_threats[0]
            competitor = threat.get('competitor', 'competitor')
            quote = threat.get('quote', '')[:80]
            reason_parts.append(f"Customer mentioned competitor '{competitor}': '{quote}...'")

        # Escalation threats
        if escalation_threats:
            threat = escalation_threats[0]
            target = threat.get('escalation_target', 'escalation')
            quote = threat.get('quote', '')[:80]
            reason_parts.append(f"Customer demanded {target}: '{quote}...'")

        # Cancellation threats
        if cancellation_threats:
            threat = cancellation_threats[0]
            source = threat.get('source', 'Interaction')
            if source == 'Call Recording':
                quote = threat.get('quote', '')[:80]
                reason_parts.append(f"Customer threatened cancellation: '{quote}...'")
            else:
                reason_parts.append(f"Customer threatened cancellation in interaction {threat.get('interaction_id', 'N/A')} on {threat['date']}")

        # Unresolved cases
        if unresolved_cases:
            case = unresolved_cases[0]
            reason_parts.append(f"{case['source']} case {case['case_id']} has been {case['status']} for {case['days_open']} days")

        # Determine priority focus based on threat type
        if legal_threats:
            priority_focus = 'LEGAL_REVIEW_REQUIRED'
        elif regulatory_threats:
            priority_focus = 'URGENT_RESOLUTION'
        elif competitor_threats:
            priority_focus = 'RETENTION_PRIORITY'
        elif cancellation_threats:
            priority_focus = 'RETENTION_PRIORITY'
        elif escalation_threats:
            priority_focus = 'ESCALATION_REQUIRED'
        elif unresolved_cases:
            priority_focus = 'ISSUE_RESOLUTION'
        else:
            priority_focus = 'ISSUE_RESOLUTION'

        # B7-H6 FIX: Use actual frustration_label in reason, not hardcoded 'high frustration'
        return {
            'safe_to_upsell': False,
            'priority_focus': priority_focus,
            'reason': f"Frustration score {frustration_score} ({frustration_label}). " + "; ".join(reason_parts) if reason_parts else f"Frustration score {frustration_score} ({frustration_label}) exceeds threshold.",
            'evidence_summary': build_evidence_summary(unresolved_cases + cancellation_threats + competitor_threats + regulatory_threats + legal_threats + escalation_threats),
            'confidence': 'HIGH'
        }

    elif frustration_score >= FRUSTRATION_THRESHOLD_MEDIUM:
        # MEDIUM frustration - VERIFY FIRST
        # BUG FIX #11: Distinguish between resolved and unresolved cases
        recently_resolved = [e for e in evidence_list if e.get('type') == 'recently_resolved_case']
        unresolved_cases = [e for e in evidence_list if e.get('type') == 'unresolved_case']

        if recently_resolved:
            case = recently_resolved[0]
            return {
                'safe_to_upsell': False,
                'priority_focus': 'VERIFY_THEN_CONSIDER',
                'condition': 'Verify case resolution and customer satisfaction first',
                # B7-H6 FIX: Use actual frustration_label
                # BUG FIX #11: Clarify that case was resolved, not that it "needs verification"
                'reason': f"Frustration score {frustration_score} ({frustration_label}). {case['source']} case {case.get('case_id', 'Unknown')} recently resolved. Verify satisfaction.",
                'evidence_summary': f"Recently resolved case - confirm satisfaction before upsell.",
                'revisit_after': '7 days after case closure',
                'confidence': 'MEDIUM'
            }

        if unresolved_cases:
            case = unresolved_cases[0]
            return {
                'safe_to_upsell': False,
                'priority_focus': 'ISSUE_RESOLUTION',
                'condition': 'Resolve case before considering upsell',
                # BUG FIX #11: Explicitly state case is unresolved (not just "needs verification")
                'reason': f"Frustration score {frustration_score} ({frustration_label}). {case['source']} case {case.get('case_id', 'Unknown')} is unresolved (open {case.get('days_open', 0)} days).",
                'evidence_summary': f"Open case - must be resolved before commercial engagement.",
                'confidence': 'MEDIUM'
            }

        return {
            'safe_to_upsell': False,
            'priority_focus': 'VERIFY_THEN_CONSIDER',
            'condition': 'Verify customer satisfaction before upsell',
            # B7-H6 FIX: Use actual frustration_label
            'reason': f"Frustration score {frustration_score} ({frustration_label}). Moderate caution advised.",
            'evidence_summary': 'Some friction detected - verify before upsell.',
            'confidence': 'MEDIUM'
        }

    else:
        # LOW frustration - GREEN LIGHT
        # Check if highly engaged (6+ contacts without cases)
        repeat_contacts = [e for e in evidence_list if e.get('type') == 'repeat_contacts']
        engagement_adjustment = None

        if repeat_contacts and repeat_contacts[0].get('count', 0) >= 6:
            # High engagement detected
            engagement_adjustment = "This is a highly engaged customer (6+ contacts in 30 days). Focus on relationship building and personalized service rather than transactional selling."

        return {
            'safe_to_upsell': True,
            'priority_focus': 'OPPORTUNITY',
            # B7-H6 FIX: Use actual frustration_label
            'reason': f"Frustration score {frustration_score} ({frustration_label}). No significant friction points.",
            'evidence_summary': 'Customer has low frustration - safe to present opportunities.',
            'engagement_adjustment': engagement_adjustment,
            'confidence': 'HIGH'
        }


def build_evidence_summary(evidence_list):
    """Build a human-readable summary of key evidence."""
    if not evidence_list:
        return "No specific evidence found."

    summaries = []
    for evidence in evidence_list[:3]:  # Top 3 items
        if evidence.get('type') == 'unresolved_case':
            summaries.append(f"Open {evidence['source']} case {evidence['case_id']} ({evidence['days_open']} days)")
        elif evidence.get('type') == 'cancellation_threat':
            interaction_id = evidence.get('interaction_id', 'Unknown')
            date = evidence.get('date', 'Unknown date')
            summaries.append(f"Cancellation threat: {interaction_id} on {date}")
        elif evidence.get('type') == 'recently_resolved_case':
            summaries.append(f"Recently resolved: {evidence['source']} case {evidence['case_id']}")
        elif evidence.get('type') == 'escalation_threat':
            # Handle escalation threats from customer quotes (may not have interaction_id)
            interaction_id = evidence.get('interaction_id', 'Unknown')
            date = evidence.get('date', 'Unknown date')
            escalation_target = evidence.get('escalation_target', 'manager')
            summaries.append(f"Escalation threat ({escalation_target}): {interaction_id} on {date}")

    return "; ".join(summaries)


# ============================================================
# DATABASE CONNECTIONS
# ============================================================

def get_connection():
    """Get database connection."""
    conn_str = ';'.join([f"{k}={v}" for k, v in DB_CONFIG.items()])
    conn_str += ';TrustServerCertificate=yes;'
    return pyodbc.connect(conn_str)


# ============================================================
# FETCH CUSTOMER PROFILE FROM REVENUE_CACHE
# ============================================================

def fetch_customer_profile(conn, customer_id):
    """Read revenue from Revenue_Cache with current fields (plan/device breakdown, NO dropped columns)."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT [customer_type], [product_list], [service_status],
               [monthly_revenue_total], [annual_revenue_total],
               [revenue_segment], [contract_end_fixed], [tenure_months],
               [has_mobile], [has_fixed], [cached_at],
               [monthly_revenue_mobile], [monthly_revenue_fixed],
               [mobile_active], [fixed_active],
               [mobile_account], [fixed_account],
               [plan_count], [account_category], [device_count], [device_financing_revenue],
               -- NEW: Plan vs Device Revenue Breakdown
               [monthly_revenue_plan], [annual_revenue_plan],
               [monthly_revenue_device], [annual_revenue_device],
               -- NEW: Dates
               [next_bill_date], [last_payment_date],
               [charge_start_date], [charge_end_date],
               -- NEW: Marketing and Service
               [marketing_consent], [service_type]
        FROM [dbo].[Revenue_Cache]
        WHERE [customer_id] = ?
    """, customer_id)
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None

    # Unpack revenue components (needed for recalculation)
    monthly_plan = float(row[21]) if row[21] else 0
    # Use monthly_revenue_device (row[23]) if available, otherwise fall back to device_financing_revenue (row[20])
    monthly_device = float(row[23]) if row[23] else (float(row[20]) if row[20] else 0)
    monthly_fixed = float(row[12]) if row[12] else 0
    annual_plan = float(row[22]) if row[22] else 0
    annual_device = float(row[24]) if row[24] else (float(row[20]) * 12 if row[20] else 0)
    annual_fixed = monthly_fixed * 12  # Approximate annual from monthly

    # Recalculate as sum of plan + device + fixed to ensure accuracy
    calculated_monthly_total = monthly_plan + monthly_device + monthly_fixed
    calculated_annual_total = annual_plan + annual_device + annual_fixed

    # Unpack all current fields
    # B7-M3 FIX: Deduplicate product_list to prevent duplicate device entries
    product_list_raw = row[1] if row[1] else ''
    if product_list_raw:
        # Split by comma, strip whitespace, deduplicate, rejoin
        products = [p.strip() for p in product_list_raw.split(',')]
        unique_products = list(dict.fromkeys(products))  # Preserve order while deduplicating
        product_list_deduped = ', '.join(unique_products)

        # Log if deduplication removed duplicates
        if len(unique_products) < len(products):
            logger.info(f"  B7-M3 FIX: Deduplicated product_list from {len(products)} to {len(unique_products)} items: '{product_list_raw}' → '{product_list_deduped}'")
        product_list = product_list_deduped
    else:
        product_list = product_list_raw

    return {
        'customer_id': customer_id,
        'customer_type': row[0],
        'product_list': product_list,
        'service_status': row[2],
        'monthly_revenue_total': calculated_monthly_total if calculated_monthly_total > 0 else None,
        'annual_revenue_total': calculated_annual_total if calculated_annual_total > 0 else None,
        'revenue_segment': row[5],
        'contract_end_fixed': str(row[6]) if row[6] else None,
        'tenure_months': row[7],
        'has_mobile': bool(row[8]),
        'has_fixed': bool(row[9]),
        'revenue_cached_at': str(row[10]) if row[10] else None,
        'monthly_revenue_mobile': float(row[11]) if row[11] else None,
        'monthly_revenue_fixed': float(row[12]) if row[12] else None,
        'mobile_active': bool(row[13]),
        'fixed_active': bool(row[14]),
        'mobile_account': row[15],
        'fixed_account': row[16],
        'plan_count': row[17],
        'account_category': row[18],
        'device_count': row[19],
        'device_financing_revenue': float(row[20]) if row[20] else None,
        # Plan vs Device Revenue Breakdown (row[21-24])
        'monthly_revenue_plan': float(row[21]) if row[21] else None,
        'annual_revenue_plan': float(row[22]) if row[22] else None,
        'monthly_revenue_device': float(row[23]) if row[23] else None,
        'annual_revenue_device': float(row[24]) if row[24] else None,
        # Note: sim_count removed - use plan_count instead (row[17])
        # Dates (row[25-28])
        'next_bill_date': str(row[25]) if row[25] else None,
        'last_payment_date': str(row[26]) if row[26] else None,
        'charge_start_date': str(row[27]) if row[27] else None,
        'charge_end_date': str(row[28]) if row[28] else None,
        # Marketing and Service (row[29-30])
        'marketing_consent': bool(row[29]) if row[29] is not None else None,
        'service_type': row[30],
    }


# ============================================================
# BUILD CUSTOMER PAYLOAD
# ============================================================

def normalize_currency_symbols(text, country_code='IE'):
    """
    B-009 FIX: Normalize currency symbols to EUR for Irish accounts.

    DEPLOYMENT NOTE: All customers in this system are Irish.
    Default country_code to 'IE' to ensure all currency symbols are normalized to €.

    If account country is IE and currency symbol is $, replace with €.
    This handles cases where IVR/payment systems report amounts in USD format.

    Args:
        text: String that may contain currency symbols
        country_code: ISO country code (defaults to 'IE' for Irish customers)

    Returns:
        Text with normalized currency symbols
    """
    if not text or not isinstance(text, str):
        return text

    # Default to Ireland for this deployment
    if not country_code:
        country_code = 'IE'

    # Normalize $ to € for Irish customers
    if country_code.upper() == 'IE' and '$' in text:
        # Replace $ with € but preserve the amount
        normalized = re.sub(r'\$', '€', text)
        if normalized != text:
            logger.info(f"  B-009 FIX: Normalized currency symbol: $ to EUR")
        return normalized

    return text


def build_customer_payload(conn, customer_id, since_timestamp=None):
    """Build complete customer payload for LLM analysis."""
    cursor = conn.cursor()
    payload = {
        "customer_id": customer_id,
        "interactions": [],
        "call_recordings": [],
        "pega_cases": [],
        "servicenow_cases": []
    }

    # Calculate date range
    if since_timestamp:
        from_date = since_timestamp
    else:
        from_date = datetime.now() - timedelta(days=WINDOW_DAYS)

    # B-009 FIX: Currency normalization for Irish customers
    # All customers in this deployment are Irish, so normalize_currency_symbols defaults to 'IE'
    # If Revenue_Cache adds a country field in the future, uncomment below:
    # customer_profile = fetch_customer_profile(cursor, customer_id)
    # country_code = customer_profile.get('country', 'IE') if customer_profile else 'IE'
    country_code = 'IE'  # All customers are Irish, default to IE

    # Fetch interactions
    cursor.execute("""
        SELECT event_id, customer_id, event_timestamp,
               event_type, event_sub_type, event_status,
               event_detail_json
        FROM dbo.Customer360_Events
        WHERE customer_id = ?
          AND event_timestamp >= ?
        ORDER BY event_timestamp DESC
    """, customer_id, from_date)

    for row in cursor.fetchall():
        # Extract wrapup_comment from event_detail_json if present
        wrapup_comment = None
        if row[6]:
            try:
                event_detail = json.loads(row[6]) if isinstance(row[6], str) else row[6]
                if isinstance(event_detail, dict):
                    wrapup_comment = event_detail.get('wrapup_comment')
                    # B-009 FIX: Normalize currency symbols in wrapup_comment
                    wrapup_comment = normalize_currency_symbols(wrapup_comment, country_code)
            except Exception:
                pass

        payload["interactions"].append({
            "interaction_id": row[0],
            "customer_id": row[1],
            "interaction_date": str(row[2]) if row[2] else None,
            "interaction_type": row[3],
            "channel": row[4],  # event_sub_type
            "agent_name": None,
            "agent_wrapup_comment": wrapup_comment,
            "resolution_status": row[5],  # event_status
            "event_detail_json": str(row[6]) if row[6] else None
        })

    # Fetch call recordings
    cursor.execute("""
        SELECT transcript_id, customer_id, call_start, audio_duration,
               call_summary, call_issues_json, call_root_causes_json, customer_quotes_json
        FROM dbo.CallTranscript
        WHERE customer_id = ?
          AND call_start >= ?
        ORDER BY call_start DESC
    """, customer_id, from_date)

    for row in cursor.fetchall():
        # B-009 FIX: Normalize currency symbols in call_summary
        call_summary = row[4]
        if call_summary:
            call_summary = normalize_currency_symbols(call_summary, country_code)

        # B-023 FIX: Filter out agent speech from customer_quotes
        # Agent wrapups can be misclassified as customer quotes in the transcription pipeline
        raw_quotes = json.loads(row[7]) if row[7] else None
        filtered_quotes = []

        if raw_quotes:
            # Agent speech patterns that indicate the text is from the agent, not the customer
            agent_speech_patterns = [
                # Agent intros and sign-offs
                r'^(thank you for calling|thanks for calling|you\'ve reached|this is|i\'m calling on behalf of)',
                # Agent status updates
                r'^(i\'ll check|let me check|i can see|i\'m looking into|i apologize)',
                # Agent closing statements
                r'^(is there anything else|can i help|anything else|i hope this helps)',
                # Technical/operational language (typically agent)
                r'^(please|kindly|you need to|you\'ll need to|we\'ll|i\'ll have to)',
                # Agent summary language
                r'^(customer called|customer stated|customer mentioned|the customer)',
            ]

            for quote in raw_quotes:
                quote_text = quote.get('text', '') if isinstance(quote, dict) else quote
                if not quote_text:
                    continue

                quote_lower = quote_text.strip().lower()

                # Check if quote matches agent speech patterns
                is_agent_speech = False
                for pattern in agent_speech_patterns:
                    if re.search(pattern, quote_lower):
                        is_agent_speech = True
                        break

                # Also check for agent-only phrases in first few words
                if not is_agent_speech:
                    # If quote starts with agent-specific phrases, it's agent speech
                    if re.match(r'^(i\'ll|let me|i can|i will|i apologize|thank you)', quote_lower):
                        is_agent_speech = True

                if not is_agent_speech:
                    # This appears to be actual customer speech
                    filtered_quotes.append(quote)
                else:
                    # Log that we filtered out agent speech
                    logger.debug(f"  B-023 FIX: Filtered agent speech from customer_quotes: '{quote_text[:60]}...'")

            # B-023 FIX: Raise DQW if we filtered out significant agent speech
            if len(filtered_quotes) < len(raw_quotes):
                filtered_count = len(raw_quotes) - len(filtered_quotes)
                if filtered_count > 0:
                    logger.warning(f"  B-023 FIX: Filtered {filtered_count} agent speech quote(s) from call recording {row[0]}")
                    # Note: We don't raise a formal DQW here as the data is being corrected at load time

        payload["call_recordings"].append({
            "recording_id": row[0],
            "customer_id": row[1],
            "call_date": str(row[2]) if row[2] else None,
            "call_duration_sec": row[3],
            "call_summary": call_summary,
            "call_issues": json.loads(row[5]) if row[5] else None,
            "call_root_causes": json.loads(row[6]) if row[6] else None,
            "customer_quotes": filtered_quotes if filtered_quotes else None  # B-023 FIX: Use filtered quotes
        })

    # Fetch Pega cases
    cursor.execute("""
        SELECT [Case ID], [Customer ID], [Case type], [Case Sub Type],
               Status, [Created Date Time], [Resolved Date Time], Agent, PegaClosureReason
        FROM dbo.L30DCases
        WHERE [Customer ID] = ?
          AND ([Created Date Time] >= ? OR [Resolved Date Time] >= ?)
        ORDER BY [Created Date Time] DESC
    """, customer_id, from_date, from_date)

    for row in cursor.fetchall():
        payload["pega_cases"].append({
            "case_id": row[0],
            "customer_id": row[1],
            "case_type": row[2],
            "case_sub_type": row[3],
            "status": row[4],
            "created_date": row[5],
            "resolved_date": row[6],
            "agent": row[7],
            "closure_reason": row[8]
        })

    # Fetch ServiceNow cases
    cursor.execute("""
        SELECT [Service Now Incident Number], CustomerID, [SNOW Status], [Case Title],
               SNSummary, [Created Date], ResolvedDateTime
        FROM dbo.L30DSNOW
        WHERE CustomerID = ?
          AND ([Created Date] >= ? OR ResolvedDateTime >= ?)
        ORDER BY [Created Date] DESC
    """, customer_id, from_date, from_date)

    for row in cursor.fetchall():
        payload["servicenow_cases"].append({
            "incident_number": row[0],
            "customer_id": row[1],
            "status": row[2],
            "title": row[3],
            "summary": row[4],
            "created_date": row[5],
            "resolved_date": row[6]
        })

    cursor.close()

    # Fetch customer profile from Revenue_Cache
    payload["customer_profile"] = None
    try:
        payload["customer_profile"] = fetch_customer_profile(conn, customer_id)
        if payload["customer_profile"]:
            p = payload["customer_profile"]
            # FIX: Use parentheses to ensure correct format specifier application
            revenue_val = p.get('monthly_revenue_total') or 0
            logger.info(f"  Revenue: EUR{revenue_val:.0f}/month "
                         f"({p.get('revenue_segment', '?')}, {p.get('customer_type', '?')})")
        else:
            logger.info(f"  Revenue: Not found in Revenue_Cache")
    except Exception as e:
        logger.warning(f"  Revenue cache read failed for {customer_id}: {e}")

    # Fetch devices from Customer_Device_Assets
    payload["devices"] = None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT device_id, customer_id, device_brand, device_model,
                   device_colour, device_memory, device_value, down_payment,
                   installment_count, installment_amount, is_contract_active,
                   contract_start_date, contract_end_date
            FROM dbo.Customer_Device_Assets
            WHERE customer_id = ?
            ORDER BY contract_end_date DESC
        """, customer_id)

        devices = []
        for row in cursor.fetchall():
            devices.append({
                "device_id": row[0],
                "customer_id": row[1],
                "device_brand": row[2],
                "device_model": row[3],
                "device_colour": row[4],
                "device_memory": row[5],
                "device_value": float(row[6]) if row[6] else None,
                "down_payment": float(row[7]) if row[7] else None,
                "installment_count": row[8],
                "monthly_installment": float(row[9]) if row[9] else None,
                "is_contract_active": bool(row[10]),
                "contract_start_date": str(row[11]) if row[11] else None,
                "contract_end_date": str(row[12]) if row[12] else None
            })

        cursor.close()

        if devices:
            payload["devices"] = devices
            logger.info(f"  Devices: {len(devices)} asset(s)")
    except Exception as e:
        logger.warning(f"  Device fetch failed for {customer_id}: {e}")

    # Extract customer quotes and threat indicators from call recordings
    # This provides CRITICAL retention and churn risk data
    try:
        quotes_data = extract_customer_quotes_and_threats(payload.get("call_recordings", []))
        if quotes_data['quotes']:
            payload["customer_quotes_analysis"] = {
                "quotes_found": len(quotes_data['quotes']),
                "competitors_mentioned": quotes_data['competitors_found'],
                "comreg_mentioned": quotes_data['comreg_mentioned'],
                "escalation_mentioned": quotes_data['escalation_mentioned'],
                "quotes": quotes_data['quotes']
            }
            logger.info(f"  Customer Quotes: {len(quotes_data['quotes'])} quote(s) extracted")
    except Exception as e:
        logger.warning(f"  Quote extraction failed for {customer_id}: {e}")

    return payload


# ============================================================
# CALCULATE DATA FRESHNESS
# ============================================================

def calculate_data_freshness(input_summary, execution_date):
    """
    Calculate how old the customer data is at the time of summary generation.
    Returns days since most recent customer interaction.
    """

    time_period = input_summary.get('time_period', {})
    if not time_period:
        return None

    latest_interaction_str = time_period.get('latest_interaction')
    if not latest_interaction_str:
        return None

    try:
        # Parse the latest interaction date
        if isinstance(latest_interaction_str, str):
            # Try different date formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y-%m-%dT%H:%M:%S']:
                try:
                    latest_interaction = datetime.strptime(latest_interaction_str[:19], fmt)
                    break
                except Exception:
                    continue
            else:
                return None
        else:
            latest_interaction = latest_interaction_str

        # Calculate days difference
        days_old = (execution_date - latest_interaction).days

        return days_old
    except Exception as e:
        logger.warning(f"Failed to calculate data freshness: {e}")
        return None


# ============================================================
# BUILD INPUT DATA SUMMARY
# ============================================================

def build_input_data_summary(payload):
    """
    Build a summary of input data used to generate the AI summary.
    This provides transparency and easy access to source data without additional DB queries.
    """
    summary = {
        'data_sources': [],
        'data_availability': {},
        'record_counts': {},
        'time_period': None
    }

    # Customer Profile
    profile = payload.get('customer_profile')
    if profile:
        summary['data_sources'].append('Revenue_Cache')
        summary['data_availability']['customer_profile'] = True
        summary['customer_profile'] = {
            'customer_id': profile.get('customer_id'),
            'customer_type': profile.get('customer_type'),
            'service_type': profile.get('service_type'),
            'revenue_segment': profile.get('revenue_segment'),
            'monthly_revenue_total': profile.get('monthly_revenue_total'),
            'sim_count': profile.get('plan_count'),  # Use plan_count
            'tenure_months': profile.get('tenure_months'),
            'handset_remaining_installments': profile.get('handset_remaining_installments'),
            'data_source': 'Revenue_Cache'
        }
    else:
        summary['data_availability']['customer_profile'] = False

    # Interactions
    interactions = payload.get('interactions', [])
    if interactions:
        summary['data_sources'].append('Interaction')
        summary['data_availability']['interactions'] = True
        summary['record_counts']['interactions'] = len(interactions)

        # Get date range
        dates = [i.get('interaction_date') for i in interactions if i.get('interaction_date')]
        if dates:
            summary['time_period'] = {
                'earliest_interaction': min(dates).strftime('%Y-%m-%d') if hasattr(min(dates), 'strftime') else str(min(dates)),
                'latest_interaction': max(dates).strftime('%Y-%m-%d') if hasattr(max(dates), 'strftime') else str(max(dates))
            }

        summary['interactions_summary'] = {
            'total_contacts': len(interactions),
            'contact_types': {},
            'channels': {},
            'earliest_date': summary['time_period']['earliest_interaction'] if summary.get('time_period') else None,
            'latest_date': summary['time_period']['latest_interaction'] if summary.get('time_period') else None
        }

        # Count by type
        for i in interactions:
            itype = i.get('interaction_type', 'Unknown')
            ichannel = i.get('channel', 'Unknown')
            summary['interactions_summary']['contact_types'][itype] = summary['interactions_summary']['contact_types'].get(itype, 0) + 1

            # CRITICAL FIX: Clean up channel data - exclude null and internal system references
            # Internal systems like "TempServiceNow_Mobile_WB" are not suitable for display
            if ichannel and ichannel != 'Unknown' and not ichannel.startswith('Temp') and not ichannel.startswith('Service'):
                summary['interactions_summary']['channels'][ichannel] = summary['interactions_summary']['channels'].get(ichannel, 0) + 1
    else:
        summary['data_availability']['interactions'] = False

    # Pega Cases
    pega_cases = payload.get('pega_cases', [])
    if pega_cases:
        summary['data_sources'].append('Pega_Case')
        summary['data_availability']['pega_cases'] = True
        summary['record_counts']['pega_cases'] = len(pega_cases)

        # Count by status
        open_cases = sum(1 for c in pega_cases if c.get('status') and 'resolved' not in c.get('status', '').lower())
        summary['pega_cases_summary'] = {
            'total_cases': len(pega_cases),
            'open_cases': open_cases,
            'resolved_cases': len(pega_cases) - open_cases,
            'data_source': 'L30DCases'
        }
    else:
        summary['data_availability']['pega_cases'] = False

    # ServiceNow Cases
    servicenow_cases = payload.get('servicenow_cases', [])
    if servicenow_cases:
        summary['data_sources'].append('ServiceNow_Case')
        summary['data_availability']['servicenow_cases'] = True
        summary['record_counts']['servicenow_cases'] = len(servicenow_cases)
    else:
        summary['data_availability']['servicenow_cases'] = False

    # Call Recordings
    call_recordings = payload.get('call_recordings', [])
    if call_recordings:
        summary['data_sources'].append('CallTranscript')
        summary['data_availability']['call_recordings'] = True
        summary['record_counts']['call_recordings'] = len(call_recordings)

        # Count quotes
        quotes_count = 0
        for recording in call_recordings:
            quotes = recording.get('customer_quotes', [])
            if quotes:
                quotes_count += len(quotes)

        summary['call_recordings_summary'] = {
            'total_calls': len(call_recordings),
            'quotes_extracted': quotes_count,
            'data_source': 'CallTranscript.customer_quotes_json'
        }
    else:
        summary['data_availability']['call_recordings'] = False

    # Customer Quotes Analysis (if available)
    quotes_analysis = payload.get('customer_quotes_analysis')
    if quotes_analysis:
        summary['customer_quotes_analysis'] = {
            'quotes_found': quotes_analysis.get('quotes_found', 0),
            'competitors_mentioned': quotes_analysis.get('competitors_mentioned', []),
            'comreg_mentioned': quotes_analysis.get('comreg_mentioned', False),
            'escalation_mentioned': quotes_analysis.get('escalation_mentioned', False)
        }

    # Devices
    devices = payload.get('devices')
    if devices:
        summary['data_sources'].append('Customer_Device_Assets')
        summary['data_availability']['devices'] = True
        summary['record_counts']['devices'] = len(devices)

        active_contracts = sum(1 for d in devices if d.get('is_contract_active'))
        summary['devices_summary'] = {
            'total_devices': len(devices),
            'active_contracts': active_contracts,
            'data_source': 'Customer_Device_Assets'
        }
    else:
        summary['data_availability']['devices'] = False

    return summary


# ============================================================
# APPLY EXPLAINABLE GATING
# ============================================================

def apply_explainable_gating(summary, payload):
    """
    Apply evidence-based gating to LLM summary.
    Extracts evidence, calculates frustration score, and enforces gating rules.
    """
    interactions = payload.get("interactions", [])
    pega_cases = payload.get("pega_cases", [])
    servicenow_cases = payload.get("servicenow_cases", [])
    call_recordings = payload.get("call_recordings", [])
    profile = payload.get("customer_profile")
    devices = payload.get("devices")

    # Extract all evidence
    all_evidence = []
    all_evidence.extend(extract_case_evidence(pega_cases, servicenow_cases))

    # CRITICAL FIX: Extract evidence from call recordings (UNRESOLVED issues!)
    call_recording_evidence = extract_call_recording_evidence(call_recordings)
    all_evidence.extend(call_recording_evidence)
    if call_recording_evidence:
        logger.info(f"  Extracted {len(call_recording_evidence)} unresolved issues from call recordings")

    all_evidence.extend(extract_interaction_evidence(interactions))
    all_evidence.extend(extract_revenue_opportunities(devices, profile))

    # NEW: Extract threat evidence from customer quotes (competitor, regulatory, escalation)
    customer_quotes_analysis = payload.get("customer_quotes_analysis")
    if customer_quotes_analysis:
        threat_evidence = extract_threat_evidence_from_quotes(customer_quotes_analysis)
        all_evidence.extend(threat_evidence)
        if threat_evidence:
            logger.info(f"  Extracted {len(threat_evidence)} threat evidence items from customer quotes")

    # Calculate frustration score
    frustration_score = calculate_frustration_score(all_evidence)

    # Determine gating decision
    gating_decision = determine_gating_decision(frustration_score, all_evidence)

    # Ensure summary has sentiment_analysis section
    if 'sentiment_analysis' not in summary:
        summary['sentiment_analysis'] = {}

    # Update sentiment analysis with evidence
    summary['sentiment_analysis']['frustration_score'] = frustration_score

    # B7-H7 FIX: Filter OUT opportunities from sentiment_analysis.evidence
    # Opportunities should only be in opportunity_actions, not in sentiment evidence
    # sentiment_analysis.evidence should only contain sentiment signals (frustration, threats, cases)
    opportunity_types = ['family_plan_opportunity', 'device_upgrade_opportunity', 'postpaid_conversion_opportunity']
    sentiment_evidence_only = [e for e in all_evidence if e.get('type') not in opportunity_types]

    # Log if any opportunities were filtered
    filtered_count = len(all_evidence) - len(sentiment_evidence_only)
    if filtered_count > 0:
        logger.info(f"  B7-H7 FIX: Filtered {filtered_count} opportunity(ies) from sentiment_analysis.evidence (opportunities go to opportunity_actions only)")

    summary['sentiment_analysis']['evidence'] = sentiment_evidence_only

    # CRITICAL FIX: Check for INSUFFICIENT_DATA scenario
    # When call recordings are missing/unanalyzed AND customer has high contact activity OR no data at all,
    # we cannot confidently determine sentiment. Mark as UNKNOWN to prevent false POSITIVE.
    contact_count = len(interactions)

    # Check if we have call recording quotes (the primary source of sentiment data)
    quotes_extracted = 0
    for recording in call_recordings or []:
        if recording.get('customer_quotes'):
            quotes_extracted += len(recording.get('customer_quotes', []))

    # Debug log for sentiment determination
    logger.info(f"  Sentiment determination: contact_count={contact_count}, quotes_extracted={quotes_extracted}, frustration_score={frustration_score}")

    # CRITICAL FIX: Check for CRITICAL/HIGH issues before defaulting to POSITIVE
    # LLM may identify CRITICAL/HIGH issues even when frustration_score is low
    has_critical_issue = False
    has_high_issue = False
    has_long_open_issue = False
    has_dissatisfaction_keywords = False
    max_days_open = 0  # CRITICAL FIX: Track actual days_open for accurate reasoning

    # Check key_issues for priority
    key_issues = summary.get('key_issues', [])
    for issue in key_issues:
        priority = issue.get('priority', '').upper()
        if priority == 'CRITICAL':
            has_critical_issue = True
        elif priority == 'HIGH':
            has_high_issue = True

        # Check for dissatisfaction keywords in issue text
        issue_text = str(issue.get('issue', '')).lower()
        if any(kw in issue_text for kw in ['dissatisf', 'unhappy', 'angry', 'frustrat', 'upset', 'annoyed', 'complain']):
            has_dissatisfaction_keywords = True

        # CRITICAL FIX: Track actual maximum days_open for accurate reasoning
        days_open = issue.get('days_open_or_resolved')
        if days_open and isinstance(days_open, (int, float)):
            if days_open > max_days_open:
                max_days_open = int(days_open)
            if days_open > 14:
                has_long_open_issue = True

    # Also check issue_resolution_actions for priority (LLLM may set it here instead)
    issue_actions = summary.get('recommended_actions', {}).get('issue_resolution_actions', [])
    for action in issue_actions:
        priority = action.get('priority', '').upper()
        if priority == 'CRITICAL':
            has_critical_issue = True
        elif priority == 'HIGH':
            has_high_issue = True

    # Check LLM's engagement_style for "Frustrated" indicators
    interaction_summary = summary.get('interaction_summary', {})
    engagement_style = interaction_summary.get('engagement_style', '').lower()
    llm_says_frustrated = 'frustrat' in engagement_style

    # Determine sentiment and frustration level based on score AND data availability
    if frustration_score >= FRUSTRATION_THRESHOLD_HIGH:
        summary['sentiment_analysis']['overall_sentiment'] = 'NEGATIVE'
        summary['sentiment_analysis']['frustration_level'] = 'HIGH'
        summary['sentiment_analysis']['primary_emotion'] = 'Frustrated'
    elif frustration_score >= FRUSTRATION_THRESHOLD_MEDIUM:
        summary['sentiment_analysis']['overall_sentiment'] = 'NEUTRAL'
        summary['sentiment_analysis']['frustration_level'] = 'MEDIUM'
        summary['sentiment_analysis']['primary_emotion'] = 'Neutral'
    else:
        # Low frustration_score - but check for CRITICAL/HIGH issues, long-open issues, or LLM frustration indicators
        # These override the low frustration_score
        if has_critical_issue or llm_says_frustrated or has_dissatisfaction_keywords:
            # CRITICAL issues or LLM says frustrated → NEGATIVE
            # BUG FIX #1: Also update frustration_score to match level (prevents mismatch)
            summary['sentiment_analysis']['frustration_score'] = max(frustration_score, FRUSTRATION_THRESHOLD_HIGH)
            summary['sentiment_analysis']['overall_sentiment'] = 'NEGATIVE'
            summary['sentiment_analysis']['frustration_level'] = 'HIGH'
            summary['sentiment_analysis']['primary_emotion'] = 'Frustrated'
            summary['sentiment_analysis']['data_confidence'] = 'MEDIUM'
            summary['sentiment_analysis']['reasoning'] = (
                f'Customer has CRITICAL priority issue(s), LLM detected frustration, or issue text indicates dissatisfaction. '
                f'Overriding low frustration_score (was {frustration_score}, now {summary["sentiment_analysis"]["frustration_score"]}) to reflect actual customer state.'
            )
            logger.info(f"  Sentiment overridden to NEGATIVE: CRITICAL issue, LLM frustration, or dissatisfaction keywords detected (frustration_score={frustration_score})")
        elif has_high_issue or has_long_open_issue or has_dissatisfaction_keywords:
            # HIGH priority, long-open issues, or dissatisfaction keywords → NEUTRAL (not satisfied, but not severely frustrated)
            # BUG FIX #1: Also update frustration_score to match level (prevents mismatch)
            summary['sentiment_analysis']['frustration_score'] = max(frustration_score, FRUSTRATION_THRESHOLD_MEDIUM)
            summary['sentiment_analysis']['overall_sentiment'] = 'NEUTRAL'
            summary['sentiment_analysis']['frustration_level'] = 'MEDIUM'
            summary['sentiment_analysis']['primary_emotion'] = 'Neutral'
            summary['sentiment_analysis']['data_confidence'] = 'MEDIUM'

            # CRITICAL FIX: Build accurate reasoning based on actual condition that triggered override
            reasons = []
            if has_high_issue:
                reasons.append('HIGH priority issue(s)')
            if has_long_open_issue:
                reasons.append(f'unresolved issue(s) open {max_days_open} days')
            if has_dissatisfaction_keywords:
                reasons.append('issue text indicates dissatisfaction')

            reason_text = ', '.join(reasons)
            summary['sentiment_analysis']['reasoning'] = (
                f'Customer has {reason_text}. '
                f'Sentiment adjusted to NEUTRAL to reflect unresolved concerns (frustration_score was {frustration_score}, now {summary["sentiment_analysis"]["frustration_score"]}).'
            )
            logger.info(f"  Sentiment overridden to NEUTRAL: {reason_text} detected (frustration_score={frustration_score})")
        elif quotes_extracted == 0:
            # B-032 FIX: INSUFFICIENT_DATA - No call recording quotes analyzed
            # Without call recording analysis, we cannot confidently determine sentiment
            # Batches B1-B5: Only marked UNKNOWN if contact_count >= 3 OR contact_count == 0
            # B-032 FIX: Batches B6+ - ALWAYS mark UNKNOWN if no quotes analyzed, regardless of contact_count
            summary['sentiment_analysis']['overall_sentiment'] = 'UNKNOWN'
            summary['sentiment_analysis']['frustration_level'] = 'LOW'
            summary['sentiment_analysis']['primary_emotion'] = 'Unknown'
            summary['sentiment_analysis']['data_confidence'] = 'LOW'
            summary['sentiment_analysis']['reasoning'] = (
                f'Insufficient data to determine sentiment: {contact_count} interaction(s), '
                f'{quotes_extracted} quotes analyzed from call recordings. '
                f'Sentiment cannot be confidently determined without voice analysis.'
            )
            logger.info(f"  B-032 FIX: Sentiment marked as UNKNOWN: {contact_count} contacts, {quotes_extracted} quotes, insufficient data")
        else:
            # B-035 FIX: Always populate reasoning field (required for POSITIVE case)
            # Only reach here if we have call recording quotes (quotes_extracted > 0)
            # This means we have actual voice analysis data to base POSITIVE sentiment on
            summary['sentiment_analysis']['reasoning'] = (
                f'Customer has low frustration score ({frustration_score}) with no significant friction points detected. '
                f'No critical issues, no long-open issues, and {quotes_extracted} quotes analyzed from call recordings.'
            )
            summary['sentiment_analysis']['overall_sentiment'] = 'POSITIVE'
            summary['sentiment_analysis']['frustration_level'] = 'LOW'
            summary['sentiment_analysis']['primary_emotion'] = 'Satisfied'
            summary['sentiment_analysis']['data_confidence'] = 'HIGH'
            logger.info(f"  Sentiment POSITIVE: {contact_count} contacts, {quotes_extracted} quotes, frustration_score={frustration_score}")

    # Apply gating to recommended_actions
    if 'recommended_actions' not in summary:
        summary['recommended_actions'] = {}

    # BUG FIX #4: Initialize unresolved_critical_high_count (will be updated later if issues exist)
    gating_decision['unresolved_critical_high_count'] = 0

    summary['recommended_actions']['action_gating'] = gating_decision
    summary['recommended_actions']['priority_focus'] = gating_decision['priority_focus']

    # CRITICAL FIX: Check LLM-identified CRITICAL/HIGH issues in issue_resolution_actions and key_issues
    # Even if frustration_score is low, if the LLM identified CRITICAL/HIGH priority issues,
    # we MUST gate the upsell to prevent inappropriate commercial guidance
    critical_blocking_issues = []

    # B-013 FIX: Deduplicate issues - same issue can appear in both key_issues and issue_resolution_actions
    # Use a set to track unique identifiers (case_id + title) to prevent double-counting
    seen_issue_identifiers = set()

    # Check issue_resolution_actions for CRITICAL/HIGH priority items
    # CRITICAL FIX: Only count OPEN issues, not RESOLVED/CLOSED ones
    issue_actions = summary.get('recommended_actions', {}).get('issue_resolution_actions', [])
    for action in issue_actions:
        priority = action.get('priority', '').upper()
        status = action.get('status', '').upper()

        # Only CRITICAL/HIGH priority AND still OPEN (not resolved/closed)
        if priority in ['CRITICAL', 'HIGH'] and 'RESOLVED' not in status and 'CLOSED' not in status:
            case_id = action.get('evidence', {}).get('case_id', 'Unknown')
            title = action.get('evidence', {}).get('title', 'Unknown issue')

            # B-013 FIX: Create unique identifier from case_id and title to prevent double-counting
            # Normalize title for comparison (strip, lowercase)
            title_normalized = title.strip().lower() if title else 'unknown'
            issue_identifier = f"{case_id}:{title_normalized}"

            if issue_identifier not in seen_issue_identifiers:
                critical_blocking_issues.append({
                    'priority': priority,
                    'title': title,
                    'case_id': case_id,
                    'source': 'issue_resolution_actions',
                    'status': status  # B-010 FIX: Store status for later resolution check
                })
                seen_issue_identifiers.add(issue_identifier)

    # Check key_issues for Open/InProgress status items
    # CRITICAL FIX: Only count issues with status containing "Open" or "In Progress", not resolved
    key_issues = summary.get('key_issues', [])
    for issue in key_issues:
        status = issue.get('status', '').upper()
        # Only include if status indicates it's still open
        if any(s in status for s in ['OPEN', 'IN PROGRESS']) and 'RESOLVED' not in status and 'CLOSED' not in status:
            issue_desc = issue.get('issue', 'Unknown issue')
            source = issue.get('source', 'Unknown')
            case_id = issue.get('case_id', '') or issue_desc  # Use case_id or description as identifier

            # B-013 FIX: Create unique identifier from case_id and title to prevent double-counting
            issue_desc_normalized = issue_desc.strip().lower() if issue_desc else 'unknown'
            issue_identifier = f"{case_id}:{issue_desc_normalized}"

            if issue_identifier not in seen_issue_identifiers:
                days_open = issue.get('days_open_or_resolved')
                critical_blocking_issues.append({
                    'priority': issue.get('priority', 'HIGH').upper(),
                    'title': issue_desc,
                    'case_id': case_id,
                    'source': source,
                    'days_open': days_open,
                    'status': status  # B-010 FIX: Store status for later resolution check
                })
                seen_issue_identifiers.add(issue_identifier)

    # B-013 FIX: Log deduplication results
    total_duplicates_prevented = len(issue_actions) + len(key_issues) - len(seen_issue_identifiers)
    if total_duplicates_prevented > 0:
        logger.info(f"  B-013 FIX: Deduplicated {total_duplicates_prevented} duplicate issue(s) - {len(critical_blocking_issues)} unique blocking issues")

    # B-010 FIX: Check if all issues are actually resolved before setting ISSUE_RESOLUTION
    # If all key_issues are Resolved, priority_focus should be VERIFY_THEN_CONSIDER, not ISSUE_RESOLUTION
    all_issues_resolved = True
    if key_issues:
        for issue in key_issues:
            status = issue.get('status', '').upper()
            # BUG FIX #15: 'Unknown' status should NOT be considered as resolved
            # Only RESOLVED or CLOSED status should count as resolved
            # Unknown, Open, In Progress should all be considered unresolved
            if status not in ['RESOLVED', 'CLOSED']:
                all_issues_resolved = False
                break

    # B7-M8 FIX: Also check issue_resolution_actions for resolution status
    issue_actions = summary.get('recommended_actions', {}).get('issue_resolution_actions', [])
    for action in issue_actions:
        status = action.get('status', '').upper()
        # BUG FIX #15: Same logic - only RESOLVED/CLOSED count as resolved
        if status not in ['RESOLVED', 'CLOSED']:
            all_issues_resolved = False
            break

    # Only apply CRITICAL/HIGH issue blocking if there are actual open issues
    if critical_blocking_issues and not all_issues_resolved:
        if gating_decision.get('safe_to_upsell'):
            logger.warning(f"  OVERRIDING safe_to_upsell: Found {len(critical_blocking_issues)} CRITICAL/HIGH issue(s)")
            for issue in critical_blocking_issues[:3]:  # Log first 3
                logger.warning(f"    - {issue}")

        gating_decision['safe_to_upsell'] = False
        gating_decision['priority_focus'] = 'ISSUE_RESOLUTION'
        # CRITICAL FIX: Also update recommended_actions.priority_focus to keep them synchronized
        summary['recommended_actions']['priority_focus'] = 'ISSUE_RESOLUTION'
        gating_decision['overridden'] = True

        # B-011 FIX: Separate reason (business-facing) from override_reason (technical note)
        # B7-M8 FIX: Check issue status before using "unresolved issues" template
        # BUG FIX: Only consider issues as resolved if status is RESOLVED or CLOSED
        resolved_blocking_issues = [i for i in critical_blocking_issues if i.get('status', '').upper() in ['RESOLVED', 'CLOSED']]
        open_blocking_issues = [i for i in critical_blocking_issues if i.get('status', '').upper() in ['OPEN', 'IN PROGRESS']]
        open_count = len(open_blocking_issues)
        resolved_count = len(resolved_blocking_issues)

        if resolved_count > 0 and open_count == 0:
            # All blocking issues are resolved - use different template
            highest_priority = critical_blocking_issues[0].get('priority', 'CRITICAL') if critical_blocking_issues else 'CRITICAL'
            issue_titles = ', '.join([str(issue.get('title', 'Unknown'))[:40] for issue in critical_blocking_issues[:3]])

            gating_decision['reason'] = (
                f"Frustration score {frustration_score} elevated despite recent resolution. "
                f"Verify satisfaction before presenting offers. "
                f"Resolved {highest_priority} priority issue(s): {issue_titles}"
            )

            gating_decision['override_reason'] = (
                f"LLM suggested safe_to_upsell=True; Python pipeline overrode to False due to elevated frustration ({summary.get('sentiment_analysis', {}).get('frustration_score', frustration_score)}). "
                f"Issues have been resolved but customer satisfaction verification required before commercial engagement."
            )
        else:
            # Use standard template for unresolved issues
            highest_priority = critical_blocking_issues[0].get('priority', 'CRITICAL') if critical_blocking_issues else 'CRITICAL'
            issue_titles = ', '.join([str(issue.get('title', 'Unknown'))[:40] for issue in critical_blocking_issues[:3]])

            # BUG FIX #4: Validate unresolved count to prevent double-counting
            # Cross-check with actual key_issues to ensure count is accurate
            actual_open_in_key_issues = 0
            for issue in summary.get('key_issues', []):
                status = issue.get('status', '').upper()
                priority = issue.get('priority', '').upper()
                if priority in ['CRITICAL', 'HIGH'] and any(s in status for s in ['OPEN', 'IN PROGRESS']) and 'RESOLVED' not in status and 'CLOSED' not in status:
                    actual_open_in_key_issues += 1

            actual_open_in_actions = 0
            for action in summary.get('recommended_actions', {}).get('issue_resolution_actions', []):
                status = action.get('status', '').upper()
                priority = action.get('priority', '').upper()
                if priority in ['CRITICAL', 'HIGH'] and 'RESOLVED' not in status and 'CLOSED' not in status:
                    actual_open_in_actions += 1

            # Use the actual count from sources, not the deduplicated array (which may still have bugs)
            actual_unresolved_count = max(actual_open_in_key_issues, actual_open_in_actions)

            if actual_unresolved_count != len(critical_blocking_issues):
                logger.warning(f"  BUG FIX #4: Unresolved count mismatch! critical_blocking_issues has {len(critical_blocking_issues)} but actual unresolved is {actual_unresolved_count}")
                # Use the actual count to be safe
                unresolved_count = actual_unresolved_count
            else:
                unresolved_count = len(critical_blocking_issues)

            # BUG FIX #4: Add unresolved_critical_high_count to action_gating for validation
            gating_decision['unresolved_critical_high_count'] = unresolved_count

            gating_decision['reason'] = (
                f"Customer has {unresolved_count} unresolved {highest_priority} priority issue(s) "
                f"that must be resolved before considering commercial offers. "
                f"Issues: {issue_titles}"
            )

            gating_decision['override_reason'] = (
                f"LLM suggested safe_to_upsell=True; Python pipeline overrode to False due to {unresolved_count} "
                f"CRITICAL/HIGH priority issue(s) detected in key_issues/issue_resolution_actions. "
                f"Highest priority: {highest_priority}"
            )

        gating_decision['blocking_issues'] = critical_blocking_issues[:5]  # Top 5 blocking issues
    elif all_issues_resolved and key_issues:
        # B-010 FIX: All issues are resolved - downgrade to VERIFY_THEN_CONSIDER
        logger.info(f"  B-010 FIX: All {len(key_issues)} issue(s) are Resolved. Downgrading priority_focus to VERIFY_THEN_CONSIDER.")
        gating_decision['safe_to_upsell'] = False  # Still verify before upsell
        gating_decision['priority_focus'] = 'VERIFY_THEN_CONSIDER'
        summary['recommended_actions']['priority_focus'] = 'VERIFY_THEN_CONSIDER'
        gating_decision['override_reason'] = 'All issues resolved. Verify customer satisfaction before considering upsell.'

        # BUG FIX #11: Update reason to reflect resolved issues, not "unresolved issues"
        # The original reason from determine_gating_decision() might say "unresolved issues" based on evidence_list
        # even though all key_issues are actually Resolved/Closed
        if 'unresolved' in str(gating_decision.get('reason', '')).lower():
            # Replace with accurate description
            highest_priority = key_issues[0].get('priority', 'HIGH') if key_issues else 'HIGH'
            issue_titles = ', '.join([str(i.get('issue', 'Unknown'))[:40] for i in key_issues[:3]])
            gating_decision['reason'] = (
                f"Frustration score {frustration_score} ({'HIGH' if frustration_score >= 60 else 'MEDIUM'}). "
                f"All {highest_priority} priority issue(s) resolved: {issue_titles}. "
                f"Verify customer satisfaction before considering upsell."
            )
            logger.info(f"  BUG FIX #11: Updated gating reason to reflect resolved issues (removed 'unresolved' terminology)")
        # BUG FIX #4: Set unresolved_critical_high_count to 0 when all issues are resolved
        gating_decision['unresolved_critical_high_count'] = 0

    # B-005, B-006 FIX: GDPR COMPLIANCE - Check marketing_consent INDEPENDENTLY of issue-blocking
    # gdpr_block must be set as a boolean field regardless of what other blocks fired
    # GDPR check is not mutually exclusive with issue-blocking - both can coexist
    marketing_consent = profile.get('marketing_consent') if profile else None

    # B-006 FIX: Treat marketing_consent=None (unknown) as no-consent
    # Check if marketing_consent is False OR None (both require gdpr_block=True)
    gdpr_block_required = marketing_consent is False or marketing_consent is None

    if gdpr_block_required:
        # B-005 FIX: Set gdpr_block regardless of safe_to_upsell value
        # GDPR block is independent and can co-exist with issue-blocking
        if not gating_decision.get('gdpr_block'):
            logger.warning(f"  GDPR COMPLIANCE: Customer has marketing_consent={marketing_consent}. Setting gdpr_block=True.")

        gating_decision['gdpr_block'] = True  # Always set as boolean
        gating_decision['safe_to_upsell'] = False  # No proactive outreach allowed

        # Set priority_focus based on whether there are also issue blocks
        if 'blocking_issues' not in gating_decision:
            # No issue blocks - pure GDPR block
            gating_decision['priority_focus'] = 'RETENTION_ONLY'
            summary['recommended_actions']['priority_focus'] = 'RETENTION_ONLY'

        gating_decision['overridden'] = True

        # B-011 FIX: Separate reason (business-facing) from override_reason (technical note)
        if marketing_consent is False:
            # Business-facing reason for agents
            gdpr_reason = (
                "Customer has opted out of marketing communications. "
                "Reactive service support is allowed, but proactive sales/upsell outreach is not permitted."
            )
            # Technical override reason for developers
            gdpr_override_reason = (
                "LLM suggested safe_to_upsell=True; Python pipeline overrode to False due to marketing_consent=False. "
                "GDPR compliance: customer has opted out of marketing communications."
            )
        else:  # marketing_consent is None
            # Business-facing reason for agents
            gdpr_reason = (
                "Customer has unknown marketing consent status (null). Treating as no-consent until verified. "
                "Reactive service support is allowed, but proactive sales/upsell outreach is not permitted."
            )
            # Technical override reason for developers
            gdpr_override_reason = (
                "LLM suggested safe_to_upsell=True; Python pipeline overrode to False due to marketing_consent=null (unknown). "
                "GDPR compliance: customer consent status unknown - treating as no-consent until verified."
            )

        # BUG FIX #15: Consolidate override_reason instead of double-concatenation
        # When both issue-based block AND GDPR block apply, consolidate into single coherent statement
        existing_override = gating_decision.get('override_reason', '')
        if existing_override:
            # Check if existing_override already contains "LLM suggested" prefix
            # If so, consolidate by combining reasons instead of duplicating the prefix
            if 'LLM suggested safe_to_upsell=True; Python pipeline overrode' in existing_override:
                # Extract the reasons from both strings and combine them
                # existing_override format: "LLM suggested... due to [REASON1]. [DETAILS]."
                # gdpr_override_reason format: "LLM suggested... due to [REASON2]. [DETAILS]."

                # Remove the common prefix and just combine the reasons
                existing_reason_part = existing_override.split('Python pipeline overrode to False due to ')[1] if 'Python pipeline overrode to False due to ' in existing_override else ''
                gdpr_reason_part = gdpr_override_reason.split('Python pipeline overrode to False due to ')[1] if 'Python pipeline overrode to False due to ' in gdpr_override_reason else gdpr_override_reason

                # Create consolidated reason
                gating_decision['override_reason'] = (
                    f"LLM suggested safe_to_upsell=True; Python pipeline overrode to False due to "
                    f"{existing_reason_part.rstrip('. ')} AND {gdpr_reason_part}"
                )
                logger.info(f"  BUG FIX #15: Consolidated double override_reason into single statement")
            else:
                # Different format - just concatenate with separator
                gating_decision['override_reason'] = existing_override + ' | ' + gdpr_override_reason
        else:
            gating_decision['override_reason'] = gdpr_override_reason

        # Set 'reason' to include GDPR information
        existing_reason = gating_decision.get('reason', '')
        if existing_reason and gdpr_reason not in existing_reason:
            gating_decision['reason'] = existing_reason + ' ' + gdpr_reason
        elif not existing_reason:
            gating_decision['reason'] = gdpr_reason

        logger.info(f"  Set gdpr_block=True (marketing_consent={marketing_consent})")

    # BUG FIX #12: Detect cancellation and set correct priority_focus
    # If customer has threatened cancellation (30-day notice processed), priority_focus should be RETENTION_ONLY
    # not VERIFY_THEN_CONSIDER or any other state
    threat_indicators = summary.get('threat_indicators', {})
    cancellation_threats = threat_indicators.get('cancellation_threats', {})
    threatened_cancellation = cancellation_threats.get('threatened_cancellation', False)

    if threatened_cancellation:
        current_priority_focus = gating_decision.get('priority_focus', '')
        # Only override if not already set to a more specific cancellation-focused state
        if current_priority_focus not in ['RETENTION_ONLY', 'LEGAL_REVIEW_REQUIRED', 'URGENT_RESOLUTION']:
            old_priority = gating_decision.get('priority_focus', '')
            gating_decision['priority_focus'] = 'RETENTION_ONLY'
            summary['recommended_actions']['priority_focus'] = 'RETENTION_ONLY'
            logger.info(f"  BUG FIX #12: Overrode priority_focus from '{old_priority}' to 'RETENTION_ONLY' (customer has threatened_cancellation=True)")


    # OVERRIDE LLM if it suggested upsell when not safe
    if not gating_decision['safe_to_upsell']:
        # B7-H5 FIX: Instead of clearing opportunity_actions, add gated flag
        # This allows agents to see opportunities but with gating context
        recommended = summary.get('recommended_actions', {})
        opportunity_actions = recommended.get('opportunity_actions', [])

        if opportunity_actions:
            # Add gated flag and gating reason to each opportunity
            gating_reason = gating_decision.get('reason', 'Upsell currently blocked due to customer concerns')
            for opp in opportunity_actions:
                opp['gated'] = True
                opp['gating_reason'] = gating_reason
                opp['display_instruction'] = f"Do not present - {gating_reason}"

            logger.info(f"  B7-H5 FIX: Added gated=True flag to {len(opportunity_actions)} opportunity actions (safe_to_upsell=False)")

        summary['recommended_actions']['opportunity_actions'] = opportunity_actions

        if 'overridden' not in gating_decision:
            gating_decision['overridden'] = True
            # B-011 FIX: Separate reason (business-facing) from override_reason (technical note)
            # B7-H6 FIX: Use actual frustration_label, not hardcoded "high frustration"
            frustration_label = 'HIGH' if frustration_score >= 60 else 'MEDIUM' if frustration_score >= 40 else 'LOW' if frustration_score >= 20 else 'VERY LOW'
            gating_decision['reason'] = (
                f"Frustration score {frustration_score} ({frustration_label}) indicates unresolved issues. "
                "Focus on resolving customer concerns first before considering commercial offers."
            )
            gating_decision['override_reason'] = (
                f"LLM suggested safe_to_upsell=True; Python pipeline overrode to False based on frustration score {frustration_score} ({frustration_label}). "
                f"{frustration_label} frustration indicates unresolved issues that must be addressed before commercial engagement."
            )

    # CRITICAL FIX: Ensure action_gating and priority_focus are synchronized with final gating_decision
    summary['recommended_actions']['action_gating'] = gating_decision
    summary['recommended_actions']['priority_focus'] = gating_decision['priority_focus']

    # B-012 FIX: Regenerate evidence_summary in action_gating AFTER overrides
    # When Python overrides safe_to_upsell to False, action_gating.evidence_summary should reflect the final decision
    if not gating_decision.get('safe_to_upsell') and gating_decision.get('overridden'):
        # Build fresh evidence_summary for action_gating based on the final gating reason (business-facing)
        reason = gating_decision.get('reason', '')
        if reason:
            gating_decision['evidence_summary'] = f"ServiceSight Intelligence: {reason}"
            logger.info(f"  B-012 FIX: Regenerated action_gating.evidence_summary after Python override")
            # B-014 FIX: Do NOT modify sentiment_analysis.evidence_summary - it should contain sentiment evidence only
            # sentiment_analysis.evidence_summary is for sentiment indicators (quotes, frustration cues)
            # action_gating.evidence_summary is for business decisions (upsell gating rationale)

        # CRITICAL FIX: Override next_best_action if it violates safe_to_upsell decision
        # NBA (Next Best Action) is generated by LLM and may suggest upsell/loyalty incentives
        # when safe_to_upsell=False. Must be overridden to match gating decision.
        recommended_actions = summary.get('recommended_actions', {})
        next_best_action = recommended_actions.get('next_best_action', '')

        if next_best_action and not gating_decision.get('safe_to_upsell'):
            # Check if NBA contains upsell-related terms
            nba_lower = next_best_action.lower()
            upsell_keywords = ['loyalty', 'incentive', 'offer', 'upgrade', 'upsell', 'discount', 'promotion']
            if any(keyword in nba_lower for keyword in upsell_keywords):
                logger.warning(f"  CRITICAL: LLM's next_best_action suggests upsell ('{next_best_action[:60]}...') but safe_to_upsell=False. Overriding.")

                # Set NBA to reflect the gating decision
                gdpr_block = gating_decision.get('gdpr_block', False)
                if gdpr_block:
                    # GDPR block - no proactive outreach allowed
                    marketing_consent = profile.get('marketing_consent') if profile else None
                    if marketing_consent is None:
                        recommended_actions['next_best_action'] = "Reactive service support only. Customer has unknown marketing consent status - no proactive outreach permitted."
                    else:
                        recommended_actions['next_best_action'] = "Reactive service support only. Customer has opted out of marketing communications - no proactive outreach permitted."
                else:
                    # Issue resolution block
                    # Count blocking issues from the gating_decision
                    blocking_issues_list = gating_decision.get('blocking_issues', [])
                    blocking_count = len(blocking_issues_list) if isinstance(blocking_issues_list, list) else 0

                    if blocking_count > 0:
                        recommended_actions['next_best_action'] = f"Resolve {blocking_count} blocking issue(s) before considering upsell. Focus on customer satisfaction first."
                    else:
                        recommended_actions['next_best_action'] = "Focus on issue resolution and customer satisfaction before considering upsell opportunities."

    return summary


# ============================================================
# DASHBOARD METRICS WITH REASONING
# ============================================================

def generate_dashboard_metrics(summary_json, payload):
    """
    Generate dashboard metrics with natural language reasoning.
    Hybrid approach: Python calculates scores (deterministic), LLM generates explanations (natural language).

    Returns: dict with dashboard_metrics containing:
        - health_score: 0-100 overall customer health
        - churn_risk: 0-100 probability of churn
        - effort_score: 0-5 customer effort score (CES)
        - escalation_risk: 0-2 likelihood of escalation
        - revenue: Monthly/annual revenue with context
        - services: Active services summary
    """
    sentiment_analysis = summary_json.get('sentiment_analysis', {})
    customer_profile = payload.get('customer_profile', {})
    interactions = payload.get('interactions', [])
    pega_cases = payload.get('pega_cases', [])
    servicenow_cases = payload.get('servicenow_cases', [])
    devices = payload.get('devices', [])
    quotes_analysis = payload.get('customer_quotes_analysis', {})

    # CRITICAL FIX: Use actual payload data for metrics, not LLM-generated values
    # Count actual open cases from both Pega and ServiceNow
    actual_open_cases = [c for c in pega_cases if c.get('status') and 'open' in c.get('status', '').lower()]
    actual_open_cases.extend([c for c in servicenow_cases if c.get('status') and 'open' in c.get('status', '').lower()])

    # For effort_score, count ALL cases (not just open) since effort is about contacts made
    # Effort happens even if case is later resolved
    all_cases = list(pega_cases) + list(servicenow_cases)

    # Extract data for metrics
    frustration_score = sentiment_analysis.get('frustration_score', 0)
    total_contacts = len(interactions)  # Use actual interaction count
    open_cases = len(actual_open_cases)  # Use actual open cases count

    # B-008 FIX: Revenue display fallback chain
    # Use the first non-zero value from: monthly_revenue_total, monthly_revenue_mobile + monthly_revenue_fixed + device_financing_revenue, monthly_revenue_plan
    # Only display €0 if ALL sources are null or zero
    monthly_revenue = customer_profile.get('monthly_revenue_total', 0) or 0

    if monthly_revenue == 0:
        # Fallback 1: Try sum of component revenues
        monthly_revenue_mobile = customer_profile.get('monthly_revenue_mobile', 0) or 0
        monthly_revenue_fixed = customer_profile.get('monthly_revenue_fixed', 0) or 0
        device_financing_revenue = customer_profile.get('device_financing_revenue', 0) or 0
        component_sum = monthly_revenue_mobile + monthly_revenue_fixed + device_financing_revenue

        if component_sum > 0:
            monthly_revenue = component_sum
            logger.info(f"  B-008 FIX: Using revenue components sum ({component_sum}) for revenue display (monthly_revenue_total was 0)")

        # Fallback 2: Try monthly_revenue_plan
        if monthly_revenue == 0:
            monthly_revenue_plan = customer_profile.get('monthly_revenue_plan', 0) or 0
            if monthly_revenue_plan > 0:
                monthly_revenue = monthly_revenue_plan
                logger.info(f"  B-008 FIX: Using monthly_revenue_plan ({monthly_revenue_plan}) for revenue display (other sources were 0)")

    # B-008 FIX: Add DQW if monthly_revenue_total=null but other revenue fields are populated
    if customer_profile.get('monthly_revenue_total') is None:
        has_other_revenue = (
            (customer_profile.get('monthly_revenue_mobile', 0) or 0) > 0 or
            (customer_profile.get('monthly_revenue_fixed', 0) or 0) > 0 or
            (customer_profile.get('device_financing_revenue', 0) or 0) > 0 or
            (customer_profile.get('monthly_revenue_plan', 0) or 0) > 0
        )
        if has_other_revenue:
            # This will be caught by validate_data_quality and logged as a DQW
            logger.warning(f"  B-008 FIX: monthly_revenue_total is null but other revenue fields exist. Using fallback value of {monthly_revenue}")

    sim_count = customer_profile.get('plan_count', 0) or 0  # Use plan_count
    device_count = len(devices) if devices else 0

    metrics = {}

    # 1. HEALTH SCORE (0-100)
    # Based on: frustration, open cases, contacts, threat indicators, mobile_active
    # B-001 FIX: Count CRITICAL and HIGH priority issues from LLM-generated key_issues
    key_issues = summary_json.get('key_issues', [])
    critical_open_count = 0
    high_open_count = 0

    for issue in key_issues:
        status = issue.get('status', '').upper()
        priority = issue.get('priority', '').upper()
        # Only count OPEN issues with CRITICAL/HIGH priority
        if status == 'OPEN' or status == 'IN PROGRESS':
            if priority == 'CRITICAL':
                critical_open_count += 1
            elif priority == 'HIGH':
                high_open_count += 1

    mobile_active = customer_profile.get('mobile_active', True)
    fixed_active = customer_profile.get('fixed_active', True)
    # B12 FIX: pass subscription flags so the penalty only fires when customer subscribed to a service
    has_mobile = customer_profile.get('has_mobile', True)
    has_fixed  = customer_profile.get('has_fixed',  True)

    health_score = calculate_health_score(
        frustration_score, open_cases, total_contacts, quotes_analysis,
        mobile_active, fixed_active, critical_open_count, high_open_count,
        has_mobile_service=has_mobile, has_fixed_service=has_fixed
    )
    metrics['health_score'] = {
        'value': health_score,
        'label': get_health_score_label(health_score),
        'color': get_health_score_color(health_score),
        'trend': {'direction': 'stable', 'indicator': '->'}  # TODO: Calculate trend from history
    }

    # 2. CHURN RISK (0-100)
    # Based on: frustration, regulatory threats, competitor mentions, single service point
    # Count CRITICAL priority cases for enhanced churn risk
    critical_cases = 0
    for case in actual_open_cases:
        priority = case.get('priority', '').upper()
        if priority in ['CRITICAL', 'HIGH']:
            critical_cases += 1

    churn_score = calculate_churn_risk(
        frustration_score, quotes_analysis, sim_count, open_cases, critical_cases, total_contacts
    )
    metrics['churn_risk'] = {
        'score': churn_score,
        'probability': get_churn_probability_label(churn_score),
        'color': get_churn_risk_color(churn_score),
        'trend': {'direction': 'stable', 'indicator': '->'}
    }

    # 3. EFFORT SCORE (0-5)
    # Based on: contacts per issue, resolution time, repeat contacts
    # CRITICAL FIX: Use ALL cases (not just open) for effort calculation
    # Effort is expended even if case is later resolved
    actual_issue_count = len(all_cases) if all_cases else 1  # Avoid division by zero
    effort_score = calculate_effort_score(
        total_contacts, actual_issue_count, pega_cases
    )
    metrics['effort_score'] = {
        'value': effort_score,
        'label': get_effort_score_label(effort_score),
        'color': get_effort_score_color(effort_score)
    }

    # 4. ESCALATION RISK (0-2)
    # Based on: threats, frustration, unresolved issues, CEO/CXO escalations
    # CRITICAL FIX: Pass evidence_list to check for CEO/CXO escalation targets
    # ESCALATION FIX: Pass threat_indicators so cancellation/switching intent is scored
    evidence_list = sentiment_analysis.get('evidence', [])
    threat_indicators = summary_json.get('threat_indicators', {})
    escalation_score = calculate_escalation_risk_score(
        quotes_analysis, frustration_score, open_cases, evidence_list, threat_indicators
    )
    metrics['escalation_risk'] = {
        'value': escalation_score,
        'label': get_escalation_risk_label(escalation_score),
        'color': get_escalation_risk_color(escalation_score)
    }

    # 5. REVENUE
    # CRITICAL FIX: Show actual revenue even for inactive customers
    # Use monthly_revenue_mobile for inactive customers (they still have revenue until fully churned)
    # This ensures agents know the customer's value even when service is temporarily inactive
    if monthly_revenue == 0 or monthly_revenue is None:
        # Try to get the mobile_revenue_mobile for inactive customers
        monthly_revenue_mobile = customer_profile.get('monthly_revenue_mobile', 0) or 0
        if monthly_revenue_mobile > 0:
            monthly_revenue = monthly_revenue_mobile
            logger.info(f"  Using monthly_revenue_mobile ({monthly_revenue_mobile}) for inactive customer revenue display")

    annual_revenue = monthly_revenue * 12 if monthly_revenue else 0
    metrics['revenue'] = {
        'monthly': round(monthly_revenue, 2) if monthly_revenue else 0,
        'monthly_display': f"€{round(monthly_revenue, 2)}" if monthly_revenue else "€0",
        'annual': round(annual_revenue, 2) if annual_revenue else 0,
        'annual_display': f"€{round(annual_revenue, 2)}" if annual_revenue else "€0",
        'color': get_revenue_color(churn_score, frustration_score)
    }

    # 6. SERVICES
    service_items = []
    mobile_active = customer_profile.get('mobile_active', False)
    fixed_active = customer_profile.get('fixed_active', False)

    # BUG FIX #8: Use mobile_active flag directly, not sim_count
    # sim_count can be 0 even when mobile_active=True (inactive SIMs but active account)
    # Services display should match mobile_active flag, not sim_count
    if mobile_active:
        service_items.append("Mobile")
    if device_count > 0:
        service_items.append("Device")
    if fixed_active:
        service_items.append("Fixed")

    # CRITICAL FIX: Determine services.status based on actual service activity, not hardcoded 'Active'
    # If no active services, status should be 'Inactive'
    # If at least one active service, status is 'Active'
    # Use mobile_active and fixed_active from customer_profile (Revenue_Cache)
    if not mobile_active and not fixed_active:
        # Both mobile and fixed are inactive
        services_status = 'Inactive'
        services_color = 'gray'
    elif service_items:
        # At least one active service
        services_status = 'Active'
        services_color = 'green'
    else:
        # Has service records but nothing active (edge case)
        services_status = 'Inactive'
        services_color = 'gray'

    metrics['services'] = {
        'items': service_items,
        'status': services_status,
        'color': services_color
    }

    return metrics


def calculate_health_score(frustration_score, open_cases, total_contacts, quotes_analysis,
                          mobile_active=True, fixed_active=True,
                          critical_open_count=0, high_open_count=0,
                          has_mobile_service=None, has_fixed_service=None):
    """
    Calculate health score (0-100, higher is better).

    Formula: Base 100 - deductions for risk factors
    - Frustration: -0.5 points per frustration score point
    - Open cases: -10 points per open case
    - High contacts: -5 points if > 5 contacts in 30 days
    - Threats: -15 points if any threats detected
    - B12 FIX: Both services inactive = Hard cap at 20 (Critical)
    - B12 FIX: ONE service inactive (only if customer SUBSCRIBED to it) = -60
    - B-001 FIX: Open CRITICAL issues = -30 each (max score 70)
    - B-001 FIX: Open HIGH issues = -15 each (max score 85 if no CRITICAL)

    B12 FIX PARAMETERS:
        has_mobile_service: True if customer ever subscribed to mobile (profile.has_mobile).
        has_fixed_service:  True if customer ever subscribed to fixed  (profile.has_fixed).
        These distinguish "never subscribed" from "subscribed but currently inactive".
        Mobile-Only customers: has_fixed_service=False → fixed_active=False is NORMAL → no penalty.
        When None (legacy callers), fall back to mobile_active/fixed_active as proxy.
    """
    health = 100

    # B12 FIX: Inactive service penalty — ONLY when the customer SUBSCRIBES to that service.
    #
    # Root cause of the original bug:
    #   `elif not mobile_active or not fixed_active: health -= 60`
    # fired for Mobile-Only customers (has_fixed=False → fixed_active always False), flooring
    # them at Warning (40) even with zero risk signals.
    #
    # Key insight: profile.has_mobile / profile.has_fixed are authoritative subscription flags.
    # mobile_active / fixed_active are the CURRENT state — only meaningful when customer
    # is actually subscribed.  If not subscribed, False is the expected normal value.
    #
    # When has_*_service is not supplied (legacy callers), treat active status as subscription proxy:
    # a caller that passes mobile_active=True, fixed_active=True (or lets them default to True)
    # will get the same behaviour as before — which is safe because that path is only hit when
    # the caller already knows both services are expected.
    eff_has_mobile = has_mobile_service if has_mobile_service is not None else mobile_active
    eff_has_fixed  = has_fixed_service  if has_fixed_service  is not None else fixed_active

    mobile_went_inactive = eff_has_mobile and not mobile_active
    fixed_went_inactive  = eff_has_fixed  and not fixed_active

    if eff_has_mobile and eff_has_fixed and not mobile_active and not fixed_active:
        # Subscribed to BOTH; both now inactive → no active services → Critical cap
        health = 20
        logger.info("  B12 FIX: Both subscribed services inactive → Critical (capped at 20)")
    elif mobile_went_inactive or fixed_went_inactive:
        inactive = []
        if mobile_went_inactive:
            inactive.append('mobile')
        if fixed_went_inactive:
            inactive.append('fixed')
        health -= 60
        logger.info(f"  B12 FIX: {', '.join(inactive)} service inactive (customer subscribed) → -60 penalty")

    # B-001 FIX: Apply penalties for open CRITICAL and HIGH priority issues
    if critical_open_count > 0:
        penalty = critical_open_count * 30
        health -= penalty
        logger.info(f"  Health score adjusted: {critical_open_count} open CRITICAL issue(s) = -{penalty} penalty")

    if high_open_count > 0:
        penalty = high_open_count * 15
        health -= penalty
        logger.info(f"  Health score adjusted: {high_open_count} open HIGH issue(s) = -{penalty} penalty")

    # Deduction for frustration
    health -= (frustration_score * 0.5)

    # Deduction for open cases (additional to priority penalties)
    health -= (open_cases * 10)

    # Deduction for high contact volume
    if total_contacts > 5:
        health -= 5

    # Deduction for threats
    if quotes_analysis:
        if quotes_analysis.get('comreg_mentioned') or \
           quotes_analysis.get('escalation_mentioned') or \
           quotes_analysis.get('legal_threat_mentioned'):
            health -= 15

    # B-001 FIX: Apply caps based on issue priority
    # If any CRITICAL issue is open, health cannot exceed 70
    if critical_open_count > 0:
        health = min(health, 70)
        logger.debug(f"  Health score capped at 70 due to {critical_open_count} open CRITICAL issue(s)")
    # If any HIGH issue is open (but no CRITICAL), health cannot exceed 85
    elif high_open_count > 0:
        health = min(health, 85)
        logger.debug(f"  Health score capped at 85 due to {high_open_count} open HIGH issue(s)")

    return max(0, min(100, int(health)))


def get_health_score_label(score):
    """Get human-readable label for health score."""
    if score >= 80:
        return "Healthy"
    elif score >= 60:
        return "Warning"
    elif score >= 40:
        return "At Risk"
    else:
        return "Critical"


def get_health_score_color(score):
    """Get color code for health score."""
    if score >= 80:
        return "green"
    elif score >= 60:
        return "orange"
    else:
        return "red"


def calculate_churn_risk(frustration_score, quotes_analysis, sim_count, open_cases, critical_cases=0, total_contacts=0):
    """
    Calculate churn risk score (0-100, higher is more risk).

    Formula: Base risk from frustration + modifiers
    - Base: frustration_score * 0.75 (increased from 0.6 - frustration is primary driver)
    - Regulatory threat: +20 points
    - Competitor mention: +15 points
    - Cancellation threat: +10 points
    - Single SIM (single point of failure): +10 points
    - Open cases: +5 per case
    - CRITICAL cases: +15 per CRITICAL case (NEW - major churn trigger)
    - High contact frequency: +10 if >=5 contacts in 30 days (NEW - escalation behavior)
    """
    risk = frustration_score * 0.75  # Increased weight - frustration is primary churn driver

    # Threat modifiers
    if quotes_analysis:
        if quotes_analysis.get('comreg_mentioned'):
            risk += 20
        if quotes_analysis.get('competitors_mentioned'):
            risk += 15
        if quotes_analysis.get('cancellation_mentioned'):
            risk += 10

    # Single service risk
    if sim_count == 1:
        risk += 10

    # Open cases risk
    risk += (open_cases * 5)

    # CRITICAL cases risk - MAJOR churn trigger
    if critical_cases > 0:
        risk += (critical_cases * 15)

    # High contact frequency - indicates escalation behavior
    if total_contacts >= 5:
        risk += 10

    return max(0, min(100, int(risk)))


def get_churn_probability_label(score):
    """Get human-readable probability label."""
    if score >= 70:
        return "High"
    elif score >= 40:
        return "Medium"
    elif score >= 20:
        return "Low"
    else:
        return "Very Low"


def get_churn_risk_color(score):
    """Get color code for churn risk."""
    if score >= 70:
        return "red"
    elif score >= 40:
        return "orange"
    elif score >= 20:
        return "yellow"
    else:
        return "green"


def calculate_effort_score(total_contacts, issue_count, pega_cases):
    """
    Calculate Customer Effort Score (0-5, lower is better).

    Formula based on CES methodology:
    - Base: 1.0 (ideal)
    - Contacts per issue: +0.5 per contact above 2
    - Open cases: +0.3 per case
    - Max score: 5.0
    """
    if issue_count == 0:
        issue_count = 1  # Avoid division by zero

    contacts_per_issue = total_contacts / issue_count
    effort = 1.0

    # Contacts per issue adds effort
    if contacts_per_issue > 2:
        effort += (contacts_per_issue - 2) * 0.5

    # Open cases add effort
    open_cases = sum(1 for case in pega_cases if case.get('status') and 'open' in case.get('status', '').lower())
    effort += (open_cases * 0.3)

    return round(min(5.0, effort), 1)


def get_effort_score_label(score):
    """Get human-readable label for effort score."""
    if score >= 4.0:
        return "Very High Effort"
    elif score >= 3.0:
        return "High Effort"
    elif score >= 2.0:
        return "Medium Effort"
    elif score >= 1.5:
        return "Low Effort"
    else:
        return "Very Low Effort"


def get_effort_score_color(score):
    """Get color code for effort score."""
    if score >= 4.0:
        return "red"
    elif score >= 3.0:
        return "orange"
    elif score >= 2.0:
        return "yellow"
    else:
        return "green"


def calculate_escalation_risk_score(quotes_analysis, frustration_score, open_cases, evidence_list=None, threat_indicators=None):
    """
    Calculate escalation risk score (0-2, higher is more risk).

    Levels: 0 = Low, 1 = Medium, 2 = High

    FIX: Now accepts threat_indicators so cancellation threats and switching intent
    are included in scoring — previously these were ignored and only explicit
    escalation_threats keywords fired.
    """
    risk = 0

    # B-015 FIX: CEO/CXO/Regulator level escalations are AUTOMATICALLY high/critical risk
    if evidence_list:
        for evidence in evidence_list:
            if evidence.get('type') == 'escalation_threat':
                escalation_target = evidence.get('escalation_target', '').lower()
                if escalation_target in ['ceo', 'cxo', 'chief executive', 'director', 'vp', 'vice president', 'manager', 'regulator', 'comreg', 'comregulation']:
                    logger.info(f"  B-015 FIX: Executive/Regulator escalation detected (target: {escalation_target}) - forcing high escalation risk")
                    return 2.0  # Maximum risk immediately

    # Explicit threat keywords
    if quotes_analysis:
        if quotes_analysis.get('escalation_mentioned') or \
           quotes_analysis.get('legal_threat_mentioned'):
            risk += 1

    # Cancellation threat = meaningful escalation signal (customer actively wants to leave)
    if threat_indicators:
        cancellation = threat_indicators.get('cancellation_threats', {})
        competitor = threat_indicators.get('competitor_threats', {})
        if cancellation.get('threatened_cancellation'):
            risk += 1.0
            logger.info("  ESCALATION FIX: threatened_cancellation=True adds +1.0 to escalation risk")
        elif competitor.get('switching_intent'):
            risk += 0.5
            logger.info("  ESCALATION FIX: switching_intent=True adds +0.5 to escalation risk")

    # High frustration = adds risk
    if frustration_score >= 60:
        risk += 0.5

    # Open cases = adds risk
    if open_cases > 0:
        risk += 0.5

    return round(min(2.0, risk), 2)


def get_escalation_risk_label(score):
    """Get human-readable label for escalation risk."""
    if score >= 1.5:
        return "High"
    elif score >= 0.8:
        return "Medium"
    else:
        return "Low"


def get_escalation_risk_color(score):
    """Get color code for escalation risk."""
    if score >= 1.5:
        return "red"
    elif score >= 0.8:
        return "orange"
    else:
        return "green"


def get_revenue_color(churn_score, frustration_score):
    """Get color code for revenue based on risk."""
    if churn_score >= 60 or frustration_score >= 60:
        return "red"
    elif churn_score >= 40 or frustration_score >= 40:
        return "orange"
    else:
        return "green"


def generate_dashboard_metrics_reasoning(client, metrics, payload, summary_json):
    """
    Generate natural language reasoning for each dashboard metric using LLM.
    Hybrid approach: Python built structured metrics → LLM refines to readable explanations.

    Args:
        client: Azure OpenAI client
        metrics: dict of calculated dashboard_metrics
        payload: input data payload
        summary_json: LLM-generated summary with sentiment_analysis, key_issues, etc.

    Returns:
        metrics dict with 'reasoning' field added to each metric
    """
    sentiment_analysis = summary_json.get('sentiment_analysis', {})
    customer_profile = payload.get('customer_profile', {})
    pega_cases = payload.get('pega_cases', [])
    servicenow_cases = payload.get('servicenow_cases', [])
    interactions = payload.get('interactions', [])
    quotes_analysis = payload.get('customer_quotes_analysis', {})

    # CRITICAL FIX: Deduplicate interactions within 30 seconds (CTI/IVR handoffs)
    # When a customer calls, the CTI system often creates 2 records: one for IVR, one for agent
    # These should be counted as 1 contact, not 2

    def deduplicate_contacts(interactions_list, gap_seconds=30):
        """Merge contacts that are within gap_seconds of each other (CTI/IVR handoffs)."""
        if not interactions_list:
            return []

        # Sort by interaction_date
        sorted_interactions = sorted(
            [i for i in interactions_list if i.get('interaction_date')],
            key=lambda x: x['interaction_date']
        )

        deduped = []

        for interaction in sorted_interactions:
            try:
                interaction_dt = datetime.fromisoformat(interaction['interaction_date'].replace('Z', '+00:00'))
            except Exception:
                # Can't parse date, include as-is
                deduped.append(interaction)
                continue

            # Check if this interaction is within gap_seconds of the last one
            # BUT only deduplicate if they are the SAME contact type
            # Phone + Inbound within 30 seconds are different channel records for the same call — keep both
            if deduped:
                try:
                    last_dt = datetime.fromisoformat(deduped[-1]['interaction_date'].replace('Z', '+00:00'))
                    time_diff = abs((interaction_dt - last_dt).total_seconds())\

                    if time_diff <= gap_seconds:
                        # Only deduplicate if same contact type (true CTI/IVR handoff)
                        last_type = (deduped[-1].get('interaction_type') or '').lower()
                        this_type = (interaction.get('interaction_type') or '').lower()
                        if last_type == this_type:
                            # True duplicate — skip
                            continue
                        # Different types within window = separate channel records — keep
                except Exception:
                    pass

            deduped.append(interaction)

        return deduped

    # Deduplicate interactions for accurate contact count
    deduped_interactions = deduplicate_contacts(interactions)
    if len(deduped_interactions) < len(interactions):
        logger.info(f"  Deduplicated {len(interactions) - len(deduped_interactions)} contact(s) within 30-second window (CTI/IVR handoffs)")

    # CRITICAL FIX: Use PAYLOAD data, not LLM-generated summary data for accuracy
    # interaction_summary and key_issues are LLM-generated and may be inconsistent
    actual_interactions_count = len(deduped_interactions)  # Use deduplicated count
    all_cases = list(pega_cases) + list(servicenow_cases)
    actual_open_cases = [c for c in all_cases if c.get('status') and 'open' in c.get('status', '').lower()]

    # Prepare context data for reasoning generation
    context = {
        'frustration_score': sentiment_analysis.get('frustration_score') or 0,  # FIX: Handle None values
        'frustration_level': sentiment_analysis.get('frustration_level') or 'UNKNOWN',  # FIX: Handle None values
        'open_cases': actual_open_cases,
        'total_contacts': actual_interactions_count,  # Use actual payload count, not LLM-generated
        'all_cases': all_cases,  # All cases (open + resolved) for effort calculation
        'unresolved_issues': actual_open_cases,  # Use actual open cases, not LLM key_issues
        'has_regulatory_threat': quotes_analysis.get('comreg_mentioned', False) if quotes_analysis else False,
        'has_competitor_threat': bool(quotes_analysis.get('competitors_mentioned', [])) if quotes_analysis else False,
        'has_escalation_threat': quotes_analysis.get('escalation_mentioned', False) if quotes_analysis else False,
        'monthly_revenue': customer_profile.get('monthly_revenue_total') or 0,  # FIX: Handle None values
        'sim_count': customer_profile.get('plan_count') or 0,  # Use plan_count (FIX: Handle None values)
        'device_count': customer_profile.get('device_count') or 0,  # B7-M5 FIX: Pass device count for services reasoning
        'device_financing_revenue': customer_profile.get('device_financing_revenue') or 0,  # B7-M5 FIX: Pass device revenue
        'interactions': interactions,  # CRITICAL FIX: Pass interactions for effort_score reasoning to check topics
        'customer_profile': customer_profile,  # CRITICAL FIX: Pass profile for DQW checks
        'summary_json': summary_json,  # CRITICAL FIX: Pass summary for data_sources_used check
        'key_issues': summary_json.get('key_issues', []),  # B7-M2 FIX: Pass key_issues for reasoning validation
    }

    # Generate reasoning for each metric
    metrics['health_score']['reasoning'] = generate_health_score_reasoning(client, metrics['health_score'], context)
    metrics['churn_risk']['reasoning'] = generate_churn_risk_reasoning(client, metrics['churn_risk'], context)
    metrics['effort_score']['reasoning'] = generate_effort_score_reasoning(client, metrics['effort_score'], context)
    metrics['escalation_risk']['reasoning'] = generate_escalation_risk_reasoning(client, metrics['escalation_risk'], context)
    metrics['revenue']['reasoning'] = generate_revenue_reasoning(client, metrics['revenue'], context)
    metrics['services']['reasoning'] = generate_services_reasoning(client, metrics['services'], context)

    return metrics


def generate_health_score_reasoning(client, health_metric, context):
    """Generate natural language reasoning for health score."""

    # B7-C2 FIX: Inject actual health_score value and label into reasoning
    # FIX: Use 'or 0' to handle case where value exists but is None
    health_value = health_metric.get('value') or 0
    health_label = health_metric.get('label') or 'Unknown'

    # Build structured reasoning (deterministic)
    factors = []
    open_case_ids = [c.get('case_id', 'Unknown') for c in context['open_cases'][:3]]  # Top 3

    if context['frustration_score'] >= 60:
        factors.append(f"HIGH frustration score ({context['frustration_score']})")

    if open_case_ids:
        if len(open_case_ids) == 1:
            factors.append(f"unresolved Pega case {open_case_ids[0]}")
        else:
            factors.append(f"{len(open_case_ids)} unresolved Pega cases")

    if context['total_contacts'] >= 5:
        factors.append(f"{context['total_contacts']} contacts in 30 days")

    if context['has_regulatory_threat']:
        factors.append("regulatory threat mentioned")

    if context['has_escalation_threat']:
        factors.append("escalation threat mentioned")

    # B7-C2 FIX: Never use "within normal range" for Critical/At Risk health scores
    # If health_label is Critical or At Risk, we MUST provide specific reasoning
    if not factors:
        if health_label in ('Critical', 'At Risk'):
            # B7-C2 FIX: Force factors for Critical/At Risk even if none detected above
            if health_value <= 20:
                factors.append(f"CRITICAL health score ({health_value}/100)")
            else:
                factors.append(f"AT RISK health score ({health_value}/100)")
        else:
            return "Customer metrics within normal range. No immediate concerns detected."

    # B7-M2 FIX: Get key_issues for validation
    key_issues = context.get('key_issues', [])

    # B7-C2 FIX: Add health_score context to prompt for LLM
    prompt = f"""Convert the following structured reasoning into a clear, 1-2 sentence explanation:

**Structured Data:**
- Health Score: {health_value}/100 ({health_label})
- Primary Factors: {', '.join(factors)}
- Frustration: {context['frustration_score']}/100 ({context['frustration_level']})
- Open Cases: {len(context['open_cases'])}
- Contacts in 30 days: {context['total_contacts']}
- Key Issues: {len(key_issues)} issue(s) detected
- B7-M2 FIX: Key Issues Data: {json.dumps(key_issues[:3]) if key_issues else 'None'}

**Requirements:**
- Write in clear, professional language
- Be specific about the issues (mention case IDs if available)
- Mention the impact on customer (risk, dissatisfaction)
- Keep it under 150 characters if possible
- No markdown, just plain text
- Make it actionable (what should be done)
- B7-C2 FIX: If health_label is 'Critical' or 'At Risk', NEVER use phrases like "within normal range" or "no immediate concerns"
- B7-C2 FIX: For Critical/At Risk scores, ALWAYS emphasize the urgency and specific issues
- B7-M2 FIX: If key_issues is non-empty, NEVER say "no issues", "despite no cases", or "within normal range"
- B7-M2 FIX: Include actual key_issues in your reasoning if present

**Example Output:**
"Customer has an unresolved Pega case (C-999162) open for 14 days with HIGH frustration score (75). Immediate resolution required to prevent churn."

Now generate the explanation:"""

    try:
        response = client.chat.completions.create(
            model=LLM_CONFIG.get("deployment_name", "gpt-4o"),
            messages=[
                {"role": "system", "content": "You are a customer experience analyst. Generate clear, concise explanations for metrics."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=150
        )
        reasoning_text = response.choices[0].message.content.strip()

        # B7-C2 FIX: Post-generation validation - fail if reasoning contradicts health_label
        if health_label in ('Critical', 'At Risk'):
            forbidden_phrases = ['within normal range', 'no immediate concerns', 'customer metrics stable', 'no issues detected']
            reasoning_lower = reasoning_text.lower()
            for phrase in forbidden_phrases:
                if phrase in reasoning_lower:
                    logger.warning(f"  B7-C2 FIX: Detected contradictory reasoning for {health_label} health score: '{phrase}' in '{reasoning_text[:80]}...'")
                    # Force fallback to factors-based reasoning
                    reasoning_text = f"Health score is {health_label} ({health_value}/100). {' '.join(factors)}. Requires urgent attention."
                    logger.info(f"  B7-C2 FIX: Corrected reasoning to: {reasoning_text}")
                    break

        # B7-M2 FIX: Validate reasoning doesn't contradict actual key_issues
        if key_issues:
            # Check if key_issues are actually open (not all resolved/closed)
            open_issues = [k for k in key_issues if k.get('status', '').upper() not in ['RESOLVED', 'CLOSED']]
            if open_issues:
                forbidden_phrases = ['no issues', 'despite no cases', 'within normal range', 'no immediate concerns']
                reasoning_lower = reasoning_text.lower()
                for phrase in forbidden_phrases:
                    if phrase in reasoning_lower:
                        logger.warning(f"  B7-M2 FIX: Reasoning says '{phrase}' but {len(open_issues)} key_issues exist")
                        # Force fallback to include key issues
                        issue_titles = [i.get('issue', 'Unknown')[:30] for i in open_issues[:2]]
                        reasoning_text = f"Health score {health_value}/100 ({health_label}). {len(open_issues)} unresolved issue(s): {', '.join(issue_titles)}."
                        logger.info(f"  B7-M2 FIX: Corrected reasoning to: {reasoning_text}")
                        break

        return reasoning_text
    except Exception as e:
        # Fallback to simple template
        return f"{' '.join(factors[:-1]) + ' and ' + factors[-1] if len(factors) > 1 else factors[0] if factors else 'Customer metrics stable'}. Requires attention."


def generate_churn_risk_reasoning(client, churn_metric, context):
    """Generate natural language reasoning for churn risk."""

    # B7-H8 FIX: Inject actual churn score, probability, and risk factors into reasoning
    churn_score = churn_metric.get('score', 0)
    churn_probability = churn_metric.get('probability', 'Unknown')

    # Determine churn risk label based on score
    if churn_score >= 70:
        churn_label = 'Very High'
    elif churn_score >= 55:
        churn_label = 'High'
    elif churn_score >= 40:
        churn_label = 'Moderate'
    elif churn_score >= 25:
        churn_label = 'Low'
    else:
        churn_label = 'Very Low'

    factors = []
    if context['has_regulatory_threat']:
        factors.append("regulatory threat (ComReg mention)")
    if context['has_competitor_threat']:
        competitors = context.get('competitors_mentioned', [])[:2]
        if competitors:
            factors.append(f"competitor mention ({', '.join(competitors)})")
    if context['frustration_score'] >= 60:
        factors.append(f"HIGH frustration ({context['frustration_score']})")
    elif context['frustration_score'] >= 40:
        factors.append(f"moderate frustration ({context['frustration_score']})")
    if context['open_cases']:
        factors.append(f"{len(context['open_cases'])} unresolved case(s)")
    if context['sim_count'] == 1:
        factors.append("single SIM (single point of failure)")

    # B7-H8 FIX: Use score-bracket-specific templates
    if not factors:
        if churn_score >= 40:
            return f"Moderate churn risk ({churn_score}/100). Monitor for changes."
        else:
            return f"Low churn risk ({churn_score}/100). Customer stable with no major concerns."

    prompt = f"""Convert the following structured reasoning into a clear, 1-2 sentence explanation:

**Structured Data:**
- Churn Risk Score: {churn_score}/100 ({churn_probability} probability - {churn_label} risk)
- Risk Factors: {', '.join(factors)}
- Revenue at Risk: €{context['monthly_revenue']}/month
- Services: {context['sim_count']} SIM(s)
- Frustration Score: {context['frustration_score']}/100

**Requirements:**
- Be specific about the threats and concerns
- Mention revenue impact if significant
- Explain WHY churn risk is {churn_label}
- Keep it under 150 characters
- No markdown, just plain text
- B7-H8 FIX: If churn_probability is 'Very Low' or 'Low', NEVER say 'High churn risk'
- B7-H8 FIX: Match the risk level to the actual churn_probability

**Example Output:**
"Regulatory threat (ComReg mention) combined with unresolved billing issue and HIGH frustration (75). High churn risk for single-SIM customer."

Now generate the explanation:"""

    try:
        response = client.chat.completions.create(
            model=LLM_CONFIG.get("deployment_name", "gpt-4o"),
            messages=[
                {"role": "system", "content": "You are a customer experience analyst. Generate clear, concise explanations for metrics."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=150
        )
        reasoning_text = response.choices[0].message.content.strip()

        # B7-H8 FIX: Post-generation validation - ensure reasoning matches actual risk level
        if churn_probability in ('Very Low', 'Low'):
            forbidden_phrases = ['high churn risk', 'elevated churn risk', 'significant churn risk']
            reasoning_lower = reasoning_text.lower()
            for phrase in forbidden_phrases:
                if phrase in reasoning_lower:
                    logger.warning(f"  B7-H8 FIX: Detected contradictory reasoning for {churn_probability} churn risk: '{phrase}'")
                    reasoning_text = f"Low churn risk ({churn_score}/100). {' '.join(factors)}." if factors else f"Low churn risk ({churn_score}/100). Customer stable."
                    break

        return reasoning_text
    except Exception as e:
        # Fallback
        return f"{' '.join(factors)}. Churn risk {churn_label} ({churn_score}/100)."


def generate_effort_score_reasoning(client, effort_metric, context):
    """Generate natural language reasoning for effort score."""

    total_contacts = context['total_contacts']
    all_cases = context.get('all_cases', [])
    open_cases = context.get('open_cases', [])  # B-004 FIX: Get open cases for accurate validation
    interactions = context.get('interactions', [])  # CRITICAL FIX: Get actual interactions

    # CRITICAL FIX: Analyze actual interaction topics to determine if they were "general inquiries"
    # or specific requests (plan changes, upgrades, etc.)
    interaction_topics = []
    for interaction in interactions:
        # Check interaction_type and agent_wrapup_comment for actual topic
        int_type_raw = interaction.get('interaction_type')
        int_type = (int_type_raw or '').lower() if int_type_raw is not None else ''
        wrapup_raw = interaction.get('agent_wrapup_comment')
        wrapup = (wrapup_raw or '') or '' if wrapup_raw is not None else ''

        # Extract topic keywords
        if any(term in int_type for term in ['plan', 'upgrade', 'change', 'modification']):
            if 'plan' not in interaction_topics:
                interaction_topics.append('plan change')
        elif any(term in int_type for term in ['billing', 'payment', 'invoice']):
            if 'billing' not in interaction_topics:
                interaction_topics.append('billing inquiry')
        elif any(term in wrapup.lower() for term in ['plan change', 'upgrade plan', 'change plan']):
            if 'plan change' not in interaction_topics:
                interaction_topics.append('plan change')

    # CRITICAL FIX: Use ALL cases (not just unresolved) for effort calculation
    # Effort is about total contacts the customer had to make, regardless of case resolution status
    total_cases = len(all_cases)
    issues_for_calc = max(1, total_cases)  # Avoid division by zero
    contacts_per_issue = round(total_contacts / issues_for_calc, 1) if total_contacts > 0 else 0

    # CRITICAL FIX: Ensure reasoning aligns with calculated score
    # Pre-generate reasoning based on the actual calculated score, not let LLM reinterpret
    score = effort_metric['value']
    label = effort_metric['label']

    # B7-M2 FIX: Get key_issues early to check for CRITICAL/HIGH priority issues
    # This prevents "No issues reported" when there are open CRITICAL/HIGH issues
    key_issues = context.get('key_issues', [])
    critical_high_issues = []
    for issue in key_issues:
        priority = issue.get('priority', '').upper()
        status = issue.get('status', '').upper()
        # Only count open CRITICAL/HIGH issues (not Resolved/Closed)
        if priority in ['CRITICAL', 'HIGH'] and status not in ['RESOLVED', 'CLOSED']:
            critical_high_issues.append(issue)

    # CRITICAL FIX: Build accurate topic description instead of using "general inquiries"
    # BUG FIX #9: When total_cases=0 but total_contacts>0, describe contacts not "0 issue(s)"
    if interaction_topics:
        topic_desc = " for ".join(interaction_topics[:2])  # Max 2 topics
        if len(interaction_topics) > 2:
            topic_desc += " requests"
    elif total_cases == 0 and total_contacts > 0:
        # Has contacts but no formal cases - describe as inquiries/requests
        topic_desc = f"various inquiries"
    elif total_cases == 0:
        topic_desc = "general inquiries"
    else:
        topic_desc = f"{total_cases} issue(s)"

    # Generate reasoning that matches the calculated score
    if score >= 4.0:
        # Very High Effort
        if total_cases > 0 and contacts_per_issue >= 4:
            reasoning = f"Customer contacted {total_contacts} times for {total_cases} issue(s) ({contacts_per_issue} contacts per issue). Very high effort indicates process failure."
        elif total_cases == 0:
            reasoning = f"Customer contacted {total_contacts} times in 30 days for {topic_desc}. Very high effort without case creation."
        else:
            reasoning = f"Customer contacted {total_contacts} times in 30 days across {total_cases} issue(s). Very high effort required."
    elif score >= 3.0:
        # High Effort
        if total_cases > 0 and contacts_per_issue >= 3:
            reasoning = f"Customer contacted {total_contacts} times for {total_cases} issue(s) ({contacts_per_issue} contacts per issue). High repeat contact pattern."
        elif total_cases == 0:
            reasoning = f"{total_contacts} contacts for {topic_desc}. High effort - cases not created despite repeat contacts."
        else:
            reasoning = f"{total_contacts} contacts to resolve {total_cases} issue(s). Above-average effort indicates friction."
    elif score >= 2.0:
        # Medium Effort
        if total_cases > 1:
            reasoning = f"Customer made {total_contacts} contacts for {total_cases} issues. Moderate effort required for resolution."
        elif total_cases == 1:
            reasoning = f"{total_contacts} contacts to resolve single issue. Moderate customer effort expended."
        else:
            # total_cases == 0
            reasoning = f"Customer made {total_contacts} contacts for {topic_desc}. Moderate effort without formal cases."
    elif score >= 1.5:
        # Low Effort
        reasoning = f"Customer made {total_contacts} contact(s) for {topic_desc}. Low effort resolution."
    else:
        # Very Low Effort
        # B7-M2 FIX: Check for open CRITICAL/HIGH issues before saying "No issues reported"
        if total_cases == 0 and not interaction_topics and critical_high_issues:
            # Has CRITICAL/HIGH key_issues but no cases - don't say "No issues reported"
            critical_count = sum(1 for i in critical_high_issues if i.get('priority') == 'CRITICAL')
            high_count = len(critical_high_issues) - critical_count
            priority_desc = f"{critical_count} CRITICAL" if critical_count > 0 else ""
            if high_count > 0:
                priority_desc += f" and {high_count} HIGH" if critical_count > 0 else f"{high_count} HIGH"
            reasoning = f"Customer made {total_contacts} contact(s). {priority_desc} issue(s) require attention."
        elif total_cases == 0 and not interaction_topics and total_contacts > 1:
            # Multiple contacts but no topics - say "various inquiries" not "0 issue(s)"
            reasoning = f"Customer made {total_contacts} contact(s) for various inquiries. Low effort."
        elif total_cases == 0 and not interaction_topics:
            reasoning = f"Customer made {total_contacts} contact(s) for general inquiries. No issues reported."
        elif total_cases == 0 and interaction_topics:
            reasoning = f"Customer made {total_contacts} contact(s) for {topic_desc}. Very low effort."
        elif contacts_per_issue <= 2:
            reasoning = f"Customer resolved {total_cases} issue(s) with {total_contacts} contact(s). Efficient, low-effort resolution."
        else:
            reasoning = f"Customer made {total_contacts} contact(s). Minimal effort required for resolution."

    # Try to enhance with LLM, but ensure score alignment
    # B7-H9 FIX: Inject actual counts explicitly and forbid LLM from changing them

    prompt = f"""The following reasoning was generated for an effort score:

**Calculated Score:** {score}/5 ({label})
**Generated Reasoning:** "{reasoning}"

**IMPORTANT - B7-H9 FIX: These values are FACTUAL and MUST NOT be changed:**
- total_contacts = {total_contacts} (use this EXACT number)
- all_cases = {len(all_cases)} (use this EXACT number)
- open_cases = {len(open_cases)} (use this EXACT number)

**B7-M2 FIX: Key Issues Data:**
- key_issues count: {len(key_issues)} issue(s)
- Key Issues: {json.dumps(key_issues[:3]) if key_issues else 'None'}

**Requirements:**
- Polish the reasoning to be clearer and more concise
- MUST NOT contradict the calculated score/label
- MUST NOT change the contact count or case count - use {total_contacts} and {len(all_cases)} exactly
- If score says "Very Low Effort", reasoning must reflect low/efficient effort
- If score says "High Effort", reasoning must reflect high/repeated effort
- Keep it under 150 characters
- No markdown, just plain text
- B7-M2 FIX: If key_issues is non-empty, NEVER say "no issues" or "despite no cases"

**Return only the refined reasoning text:**"""

    try:
        response = client.chat.completions.create(
            model=LLM_CONFIG.get("deployment_name", "gpt-4o"),
            messages=[
                {"role": "system", "content": "You are a customer experience analyst. Generate clear, concise explanations for metrics."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=150
        )
        llm_reasoning = response.choices[0].message.content.strip()

        # CRITICAL FIX: Validate LLM reasoning doesn't contradict the calculated score OR actual data
        # If LLM says "high effort" but score says "Very Low Effort", use our pre-generated reasoning
        # Also check for factually incorrect statements like "no unresolved issues" when open_cases > 0
        llm_lower = llm_reasoning.lower()

        # B7-H9 FIX: Check for wrong contact count in LLM reasoning
        # Extract numbers from LLM reasoning and verify against actual total_contacts
        llm_numbers = re.findall(r'\b\d+\b', llm_reasoning)
        for num_str in llm_numbers:
            num = int(num_str)
            # If LLM mentioned a contact count that differs from actual by more than 1
            if num >= 3 and abs(num - total_contacts) > 1:
                logger.warning(f"  B7-H9 FIX: LLM reasoning has wrong contact count: {num} vs actual {total_contacts}. Using Python-generated reasoning.")
                return reasoning

        # Check 1: Contradicts calculated score
        if score < 2.0 and any(phrase in llm_lower for phrase in ['high effort', 'very high', 'repeat', 'multiple', 'inefficiency', 'friction']):
            # Score says Very Low/Low Effort but LLM says high effort → use our reasoning
            logger.warning(f"  LLM effort reasoning contradicts score {score}/{label}. Using Python-generated reasoning.")
            return reasoning
        elif score >= 3.0 and any(phrase in llm_lower for phrase in ['low effort', 'efficient', 'minimal', 'no friction']):
            # Score says High/Very High Effort but LLM says low effort → use our reasoning
            logger.warning(f"  LLM effort reasoning contradicts score {score}/{label}. Using Python-generated reasoning.")
            return reasoning

        # Check 2: Factually incorrect statements about data
        # B-004 FIX: If LLM says "no unresolved issues" but there are actually OPEN cases, use Python reasoning
        # Use len(open_cases) not total_cases, because resolved cases don't contradict "no unresolved issues"
        if len(open_cases) > 0 and any(phrase in llm_lower for phrase in ['no unresolved issues', 'no issues', 'resolved all', 'no open cases']):
            logger.warning(f"  LLM effort reasoning factually incorrect: claims 'no issues' but {len(open_cases)} UNRESOLVED issue(s) exist. Using Python-generated reasoning.")
            return reasoning
        # If LLM says "3 contacts" but actual count is different (allow small variance for summarization)
        elif f'{total_contacts} contact' not in llm_lower:
            # LLM may have miscounted, check if it's significantly wrong
            contact_matches = re.findall(r'\d+\s*contact', llm_lower)
            if contact_matches:
                llm_contact_count = int(contact_matches[0].split()[0])
                if abs(llm_contact_count - total_contacts) > 1:  # More than 1 contact difference
                    logger.warning(f"  LLM effort reasoning has incorrect contact count: LLM said {llm_contact_count}, actual is {total_contacts}. Using Python-generated reasoning.")
                    return reasoning

        # B7-M2 FIX: Validate effort reasoning doesn't contradict actual key_issues
        if key_issues:
            # Check if key_issues are actually open (not all resolved/closed)
            open_issues = [k for k in key_issues if k.get('status', '').upper() not in ['RESOLVED', 'CLOSED']]
            if open_issues:
                forbidden_phrases = ['no issues', 'despite no cases', 'despite 0 issues']
                for phrase in forbidden_phrases:
                    if phrase in llm_lower:
                        logger.warning(f"  B7-M2 FIX: Effort reasoning says '{phrase}' but {len(open_issues)} key_issues exist")
                        # Force fallback to include key issues
                        return reasoning

        return llm_reasoning
    except Exception as e:
        # Fallback to pre-generated reasoning
        return reasoning


def generate_escalation_risk_reasoning(client, escalation_metric, context):
    """Generate natural language reasoning for escalation risk."""

    factors = []
    if context['has_escalation_threat']:
        factors.append("manager escalation threatened")
    if context['has_regulatory_threat']:
        factors.append("regulatory body mentioned")
    if context['frustration_score'] >= 60:
        factors.append(f"HIGH frustration ({context['frustration_score']})")

    if not factors:
        return "No escalation threats detected. Low risk."

    prompt = f"""Convert the following structured reasoning into a clear, 1-2 sentence explanation:

**Structured Data:**
- Escalation Risk Score: {escalation_metric['value']}/2 ({escalation_metric['label']})
- Risk Factors: {', '.join(factors)}
- Open Cases: {len(context['open_cases'])}

**Requirements:**
- Be specific about the threats
- Explain what would de-escalate
- Keep it under 150 characters
- No markdown, just plain text

**Example Output:**
"Manager escalation threat detected in recent call. Combined with HIGH frustration (75), elevated but not critical risk. Resolve billing issue to de-escalate."

Now generate the explanation:"""

    try:
        response = client.chat.completions.create(
            model=LLM_CONFIG.get("deployment_name", "gpt-4o"),
            messages=[
                {"role": "system", "content": "You are a customer experience analyst. Generate clear, concise explanations for metrics."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=150
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        # Fallback
        return f"{', '.join(factors)}. {'Elevated risk' if escalation_metric['value'] >= 1 else 'Moderate risk'}. Resolution recommended."


def generate_revenue_reasoning(client, revenue_metric, context):
    """Generate natural language reasoning for revenue."""

    monthly = context['monthly_revenue']
    annual = monthly * 12 if monthly else 0
    frustration = context['frustration_score']
    # B10 FIX: read the computed revenue_segment so the LLM uses the canonical label
    # Previously the prompt had no mention of revenue_segment, so the LLM invented its own
    # relative language ("Moderate") that contradicted the Python-computed segment label.
    customer_profile = context.get('customer_profile', {})
    revenue_segment = customer_profile.get('revenue_segment') or 'Unknown'

    if frustration >= 60:
        impact_desc = "HIGH frustration makes this customer a priority despite revenue level"
    elif frustration >= 40:
        impact_desc = "elevated frustration requires attention to protect revenue"
    else:
        impact_desc = "stable customer with normal engagement"

    prompt = f"""Convert the following structured reasoning into a clear, 1-2 sentence explanation:

**Structured Data:**
- Monthly Revenue: €{monthly}
- Annual Revenue: €{annual}
- Revenue Segment (AUTHORITATIVE): {revenue_segment} — USE THIS EXACT LABEL when describing this customer's revenue tier. Do NOT substitute relative words like "Moderate" or "Low" if the segment says "High Value".
- Frustration Score: {frustration}/100
- SIM Count: {context['sim_count']}

**Requirements:**
- Use the Revenue Segment label verbatim (e.g. "High Value customer (€{monthly}/month)")
- Contextualize revenue with risk level
- Mention if quick resolution is critical
- Keep it under 120 characters
- No markdown, just plain text

**Example Output:**
"High Value customer (€295/month) with HIGH frustration — quick resolution critical to protect €3,540/year revenue."

Now generate the explanation:"""

    try:
        response = client.chat.completions.create(
            model=LLM_CONFIG.get("deployment_name", "gpt-4o"),
            messages=[
                {"role": "system", "content": "You are a customer experience analyst. Generate clear, concise explanations for metrics. Always use the exact Revenue Segment label provided — never substitute your own wording."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=120
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        # Fallback — also use the segment label
        return f"{revenue_segment} customer (€{monthly}/month). {impact_desc}."


def generate_services_reasoning(client, services_metric, context):
    """Generate natural language reasoning for services."""

    # B11 FIX: Use customer_profile.plan_count as the single, authoritative sim_count.
    # Root cause: context['sim_count'] was set from customer_profile.plan_count in
    # generate_dashboard_metrics_reasoning(), but the LLM prompt also sees portfolio_context
    # which may describe a *different* SIM count depending on how it was assembled.
    # Result: reasoning said "9 SIMs" while elsewhere the summary said "Single SIM customer".
    # Fix: derive sim_count directly from customer_profile (authoritative DB source) and
    # label it explicitly so the LLM cannot substitute a different value from context.
    customer_profile = context.get('customer_profile', {})
    actual_sim_count = customer_profile.get('plan_count') if customer_profile else context.get('sim_count', 0)
    if actual_sim_count is None:
        actual_sim_count = 0

    # B7-M5 FIX: Get device context
    device_count = context.get('device_count', 0)
    device_revenue = context.get('device_financing_revenue', 0)

    # B7-M5 FIX: Build device context for reasoning
    device_context = []
    if device_count > 0:
        device_context.append(f"{device_count} active device contract(s)")
    if device_revenue > 0:
        device_context.append(f"€{device_revenue:.2f}/month in device revenue")

    device_desc = ', '.join(device_context) if device_context else 'No device contracts'

    prompt = f"""Convert the following structured reasoning into a clear, 1-2 sentence explanation:

**Structured Data:**
- Services: {', '.join(services_metric['items']) if services_metric['items'] else 'None'}
- SIM / Plan Count (AUTHORITATIVE, from billing system): {actual_sim_count} — USE THIS EXACT NUMBER. Do NOT use a different SIM count from any other context.
- Device Contracts: {device_desc}
- Status: {services_metric['status']}

**Requirements:**
- Mention service portfolio composition
- Note if single point of failure exists
- Keep it under 100 characters
- No markdown, just plain text
- B7-M5 FIX: If device_count > 0, mention device contracts in the reasoning
- B7-M5 FIX: NEVER say "no device contracts" if device_count > 0

**Example Output:**
"Single SIM with no device contracts. Technical service is Active. Focus on resolving billing issue to protect this single revenue stream."

Now generate the explanation:"""

    try:
        response = client.chat.completions.create(
            model=LLM_CONFIG.get("deployment_name", "gpt-4o"),
            messages=[
                {"role": "system", "content": "You are a customer experience analyst. Generate clear, concise explanations for metrics. Always use the exact SIM/Plan count provided — never substitute a different number."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=100
        )
        reasoning_text = response.choices[0].message.content.strip()

        # B7-M5 FIX: Validate services reasoning doesn't contradict device data
        if device_count > 0 and 'no device contracts' in reasoning_text.lower():
            logger.warning(f"  B7-M5 FIX: Reasoning says 'no device contracts' but device_count={device_count}")
            # Force fallback with correct device info
            items = ', '.join(services_metric['items']) if services_metric['items'] else 'Mobile service'
            return f"{items} with {device_count} device contract(s). Active service."

        return reasoning_text
    except Exception as e:
        # Fallback
        items = ', '.join(services_metric['items']) if services_metric['items'] else 'No services'
        if sim_count == 1:
            return f"Single service ({items}). Resolve issues to protect only revenue stream."
        else:
            return f"{items} portfolio. Diversified services reduce churn risk."


# ============================================================
# DETERMINE SCENARIO & FETCH EXISTING SUMMARY
# ============================================================

def determine_scenario_and_fetch_summary(conn, customer_id, watermark, run_date):
    """
    Determine processing scenario and fetch existing summary if available.

    Returns: (scenario, watermark, existing_summary_json)
      scenario: 'FULL' | 'INCREMENTAL' | 'REBUILD'
    """
    cursor = conn.cursor()

    # Query for existing summary
    cursor.execute("""
        SELECT last_processed_event_ts, summary_json,
               last_full_build_date
        FROM dbo.LLM_Customer_Summary
        WHERE customer_id = ?
    """, customer_id)

    row = cursor.fetchone()
    cursor.close()

    # Scenario 1: No row or empty content → FULL
    if row is None:
        return 'FULL', None, None

    last_processed, existing_json, last_full_build = row
    if existing_json is None:
        return 'FULL', None, last_processed

    try:
        existing_summary_json = json.loads(existing_json) if isinstance(existing_json, str) else existing_json
    except Exception:
        return 'FULL', None, last_processed

    # Scenario 3a: No last_full_build_date → REBUILD
    if last_full_build is None:
        # B-039 FIX: Check data freshness before deciding REBUILD
        # If data is more than 7 days old, trigger FULL instead of REBUILD
        data_as_of = existing_summary_json.get('input_data_summary', {}).get('time_period', {}).get('latest_interaction')
        if data_as_of:
            try:
                data_date = datetime.strptime(data_as_of[:10], '%Y-%m-%d')
                days_since_data = (run_date - data_date).days if run_date else 0
                if days_since_data > 7:
                    logger.warning(f"  B-039 FIX: Data freshness SLA violation - {days_since_data} days stale (max 7). Forcing FULL regeneration instead of REBUILD.")
                    return 'FULL', None, existing_summary_json
            except Exception:
                pass  # If we can't parse the date, proceed with REBUILD

        logger.info(f"  REBUILD: No last_full_build_date for {customer_id}")
        return 'REBUILD', last_processed, existing_summary_json

    # Scenario 3b: Periodic rebuild governance (>30 days since last full)
    try:
        if isinstance(last_full_build, str):
            last_full = datetime.strptime(last_full_build[:10], '%Y-%m-%d')
        else:
            last_full = last_full_build

        days_since_full = (run_date - last_full).days if run_date else 30
        # Uses module-level REBUILD_INTERVAL_DAYS constant (30 days)

        # B-039 FIX: Check data freshness before REBUILD
        # Enforce 7-day freshness SLA - if data is stale, do FULL instead of REBUILD
        data_as_of = existing_summary_json.get('input_data_summary', {}).get('time_period', {}).get('latest_interaction')
        if data_as_of and days_since_full <= REBUILD_INTERVAL_DAYS:
            try:
                data_date = datetime.strptime(data_as_of[:10], '%Y-%m-%d')
                days_since_data = (run_date - data_date).days if run_date else 0
                if days_since_data > 7:
                    logger.warning(f"  B-039 FIX: Data freshness SLA violation - {days_since_data} days stale (max 7). Forcing FULL regeneration instead of REBUILD.")
                    return 'FULL', None, existing_summary_json
            except Exception:
                pass  # If we can't parse the date, proceed with REBUILD

        if days_since_full > REBUILD_INTERVAL_DAYS:
            logger.info(f"  REBUILD: {days_since_full} days since last full build for {customer_id}")
            return 'REBUILD', last_processed, existing_summary_json
    except Exception as e:
        logger.warning(f"  Could not calculate rebuild interval: {e}")

    # Scenario 3c: Stale watermark
    if watermark is None:
        return 'REBUILD', None, existing_summary_json

    # Scenario 2: Valid watermark within window → INCREMENTAL
    return 'INCREMENTAL', watermark, existing_summary_json


def call_llm_incremental(client, existing_summary_json, payload, run_date):
    """
    Incremental merge: Combine existing summary with new data.
    This preserves historical context while adding new information.
    """

    # Build delta payload (only new data since watermark)
    delta_payload = {
        "new_events_since_last_summary": payload,
        "last_summary_date": existing_summary_json.get('summary_generated_at', 'Unknown'),
        "merge_instructions": """
IMPORTANT: Merge the new events into the existing summary following these rules:
1. KEEP all historical context from existing summary
2. ADD new interactions to the timeline (chronological order)
3. UPDATE open cases: if a case now appears resolved, move it to resolved_cases
4. UPDATE sentiment and frustration based on COMBINED data (old + new)
5. ADD new customer quotes and threat indicators
6. REWRITE key_issues to reflect CURRENT state (most important issues NOW)
7. REWRITE recommended_actions based on current combined state
8. UPDATE all counts (total_contacts_30d, etc.) with new data
9. PRESERVE account_value and customer info from existing summary
10. Set summary_generated_at and summary_for_date to current values

Output the COMPLETE updated summary as valid JSON.
"""
    }

    # Build combined message for LLM
    existing_size = len(existing_summary_json)
    delta_size = len(json.dumps(delta_payload, ensure_ascii=False, default=str))

    combined = (
        f"=== EXISTING SUMMARY (Context from previous summaries) ===\n{existing_summary_json}\n\n"
        f"=== NEW EVENTS SINCE LAST SUMMARY ===\n{json.dumps(delta_payload, ensure_ascii=False, default=str)}"
    )

    # Truncate if too large
    MAX_PAYLOAD_CHARS = 120000
    if len(combined) > MAX_PAYLOAD_CHARS:
        logger.warning(f"  Combined payload too large ({len(combined)} chars), truncating...")
        available = MAX_PAYLOAD_CHARS - existing_size - 200
        delta_payload_truncated = json.dumps(delta_payload, ensure_ascii=False, default=str)[:available] + "\n... [TRUNCATED]"
        combined = (
            f"=== EXISTING SUMMARY ===\n{existing_summary_json}\n\n"
            f"=== NEW EVENTS ===\n{delta_payload_truncated}"
        )

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": combined}
    ]

    start_time = time.time()

    try:
        response = client.chat.completions.create(
            messages=messages,
            model=LLM_CONFIG.get("deployment_name", "gpt-4o"),
            max_tokens=LLM_CONFIG.get("max_tokens", 16000),
            temperature=LLM_CONFIG.get("temperature", 0.1)
        )

        summary_text = response.choices[0].message.content

        # Extract usage
        usage = response.usage if hasattr(response, 'usage') else None
        input_tokens = usage.prompt_tokens if usage else 0
        output_tokens = usage.completion_tokens if usage else 0
        total_tokens = usage.total_tokens if usage else 0

        duration = time.time() - start_time

        logger.info(f"  Tokens: {input_tokens:,} in, {output_tokens:,} out, {total_tokens:,} total (INCREMENTAL)")

        # Calculate cost
        input_cost = (input_tokens / 1_000_000) * COST_PER_1M_INPUT
        output_cost = (output_tokens / 1_000_000) * COST_PER_1M_OUTPUT
        total_cost = input_cost + output_cost

        logger.info(f"  Cost: ${input_cost:.4f} + ${output_cost:.4f} = ${total_cost:.4f}")

        return {
            'summary_text': summary_text,
            'input_tokens': input_tokens,
            'output_tokens': output_tokens,
            'total_tokens': total_tokens,
            'input_cost': input_cost,
            'output_cost': output_cost,
            'total_cost': total_cost,
            'duration': duration
        }

    except Exception as e:
        logger.error(f"  INCREMENTAL API error: {e}")
        raise


# ============================================================
# CALL LLM API
# ============================================================

def call_llm_api(client, payload, run_date):
    """Call Azure OpenAI API with retry logic."""

    # Truncate payload if too large
    payload_str = json.dumps(payload, ensure_ascii=False, default=str)
    if len(payload_str) > MAX_PAYLOAD_CHARS:
        logger.warning(f"Payload too large ({len(payload_str)} chars), truncating...")
        payload_str = payload_str[:MAX_PAYLOAD_CHARS]
        payload = json.loads(payload_str)

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)}
    ]

    for attempt in range(MAX_API_RETRIES):
        try:
            logger.debug(f"LLM API call (attempt {attempt + 1}/{MAX_API_RETRIES})")

            # Build API parameters - map deployment_name to model for Azure OpenAI
            api_params = {
                "model": LLM_CONFIG.get("deployment_name", "gpt-4o"),
                "max_tokens": LLM_CONFIG.get("max_tokens", 16000),
                "temperature": LLM_CONFIG.get("temperature", 0.1)
            }

            response = client.chat.completions.create(
                messages=messages,
                **api_params
            )

            # Extract response
            summary_text = response.choices[0].message.content

            # Token usage
            usage = response.usage
            input_tokens = usage.prompt_tokens if usage else 0
            output_tokens = usage.completion_tokens if usage else 0
            total_tokens = usage.total_tokens if usage else 0

            # Calculate cost
            input_cost = (input_tokens / 1_000_000) * COST_PER_1M_INPUT
            output_cost = (output_tokens / 1_000_000) * COST_PER_1M_OUTPUT
            total_cost = input_cost + output_cost

            logger.info(f"  Tokens: {input_tokens:,} in, {output_tokens:,} out, {total_tokens:,} total")
            logger.info(f"  Cost: ${input_cost:.4f} + ${output_cost:.4f} = ${total_cost:.4f}")

            return {
                'summary_text': summary_text,
                'input_tokens': input_tokens,
                'output_tokens': output_tokens,
                'total_tokens': total_tokens,
                'input_cost': input_cost,
                'output_cost': output_cost,
                'total_cost': total_cost
            }

        except Exception as e:
            is_retryable = any(err in str(e).lower() for err in ['429', '500', '502', '503', 'timeout'])
            if is_retryable and attempt < MAX_API_RETRIES - 1:
                delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
                logger.warning(f"  API error (retryable): {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"  API error (fatal): {e}")
                raise

    raise Exception("Max retries exceeded")


# ============================================================
# DATA QUALITY VALIDATION
# ============================================================

def validate_data_quality(summary_json, payload):
    """
    Validate data quality and add warnings for pipeline issues.
    Detects when data exists but wasn't processed correctly.
    """
    warnings = []

    # Check 1: Call recordings present but no quotes extracted
    call_recordings = payload.get('call_recordings', [])
    if call_recordings:
        # Count quotes
        quotes_count = 0
        for recording in call_recordings:
            quotes = recording.get('customer_quotes', [])
            if quotes:
                quotes_count += len(quotes)

        if quotes_count == 0:
            warnings.append({
                'type': 'CALL_RECORDINGS_NOT_ANALYZED',
                'severity': 'HIGH',
                'description': f'{len(call_recordings)} call recording(s) present in CallTranscript table but no customer quotes extracted',
                'impact': 'Primary sentiment data source missing - analysis based on interaction wrap-ups only, not customer verbatim',
                'recommendation': 'Check CallTranscript.customer_quotes_json - quotes extraction pipeline may have failed. Call recordings contain raw audio/text that should have been analyzed for quotes.',
                'data_source': 'CallTranscript',
                'record_count': len(call_recordings),
                'expected_field': 'customer_quotes_json',
                'actual_value': 'null or empty'
            })
            logger.warning(f"  DATA QUALITY WARNING: {len(call_recordings)} call recordings found but 0 quotes extracted")

        # Check 2: Quotes count mismatch - LLM dropped quotes from customer_quotes array
        # call_recordings_summary.quotes_extracted counts actual quotes in payload
        # customer_quotes array is LLM's output - should match or be close
        llm_customer_quotes = summary_json.get('customer_quotes', [])
        if isinstance(llm_customer_quotes, list):
            llm_quotes_count = len(llm_customer_quotes)
            # Only warn if there's a significant mismatch (more than 1 quote dropped)
            if quotes_count > llm_quotes_count + 1:
                dropped = quotes_count - llm_quotes_count
                warnings.append({
                    'type': 'QUOTES_COUNT_MISMATCH',
                    'severity': 'MEDIUM',
                    'description': f'{quotes_count} quotes extracted from call recordings but only {llm_quotes_count} in customer_quotes output - {dropped} quote(s) dropped by LLM',
                    'impact': 'Customer voice data lost - some customer quotes from call recordings were not included in analysis. This may reduce sentiment accuracy and miss important customer statements.',
                    'recommendation': f'LLM output filtering removed {dropped} quote(s). Check LLM prompt or context limit. CallTranscript.customer_quotes_json contains all {quotes_count} quotes, but customer_quotes[] only has {llm_quotes_count}.',
                    'data_source': 'LLM response parsing',
                    'expected_count': quotes_count,
                    'actual_count': llm_quotes_count,
                    'dropped_count': dropped
                })
                logger.warning(f"  DATA QUALITY WARNING: Quotes count mismatch - {quotes_count} extracted, {llm_quotes_count} in LLM output ({dropped} dropped)")

    # Check 3: Agent-authored content misclassified as customer quotes
    # Detect agent wrapup text/shorthand that's been attributed to customer
    # This runs REGARDLESS of call_recordings presence - agent wrapup can come from interactions too
    llm_customer_quotes = summary_json.get('customer_quotes', [])
    if isinstance(llm_customer_quotes, list) and llm_customer_quotes:
        # B-025 FIX: Expanded agent shorthand patterns
        agent_shorthand_patterns = [
            r'\bcx\b',  # "cx" = customer (agent shorthand)
            r'\bcust\b',  # "cust" = customer
            r'\bcust\.',  # "cust." = customer
            r'customer advised',
            r'agent noted',
            r'wrapup:',
            r'summary:',
            r'\[agent\]',
            r'\[csr\]',  # Customer Service Representative
            # B-025 FIX: Additional patterns
            r'customer said',
            r'advised cust',
            r'inbound call',
            r'dpa passed',
            r'reason for calling:',
            r'what checks have i done:',
            r'outcome of the',
        ]

        misclassified_quotes = []
        valid_quotes = []
        misclassified_indices = []

        for idx, quote in enumerate(llm_customer_quotes):
            quote_text = quote.get('quote', '')
            if not isinstance(quote_text, str):
                valid_quotes.append(quote)
                continue

            quote_lower = quote_text.lower()
            is_misclassified = False

            for pattern in agent_shorthand_patterns:
                if re.search(pattern, quote_lower, re.IGNORECASE):
                    misclassified_quotes.append({
                        'quote': quote_text[:100],
                        'pattern_matched': pattern,
                        'index': idx
                    })
                    is_misclassified = True
                    break  # Only count each quote once

            if is_misclassified:
                misclassified_indices.append(idx)
            else:
                valid_quotes.append(quote)

        # B-023 FIX: Remove misclassified quotes from customer_quotes array
        if misclassified_quotes:
            # Update the summary_json to remove contaminated quotes
            summary_json['customer_quotes'] = valid_quotes
            logger.info(f"  B-023 FIX: Removed {len(misclassified_quotes)} agent-authored quote(s) from customer_quotes. Remaining: {len(valid_quotes)}")

            warnings.append({
                'type': 'AGENT_AUTHORED_CONTENT_MISCLASSIFIED',
                'severity': 'MEDIUM',
                'description': f'{len(misclassified_quotes)} quote(s) appear to be agent-authored wrapup text, not customer verbatim speech. Agent shorthand (e.g., "cx" for customer) detected in quote text. Quotes removed from customer_quotes array.',
                'impact': 'Agent perspective was being presented as customer voice. Contaminated quotes have been removed. sentiment_analysis.evidence_summary now only contains customer verbatim speech.',
                'recommendation': 'Review CallTranscript.customer_quotes_json extraction logic. Quotes should only contain verbatim customer speech from call transcripts, not agent wrapup_comment field or agent-authored summaries.',
                'data_source': 'CallTranscript or Customer360_Events (likely wrapup_comment field)',
                'misclassified_count': len(misclassified_quotes),
                'quotes_removed': True,
                'remaining_count': len(valid_quotes),
                'examples': misclassified_quotes[:3]  # First 3 examples
            })
            logger.warning(f"  DATA QUALITY WARNING: {len(misclassified_quotes)} quote(s) appear to be agent-authored, not customer verbatim - REMOVED from customer_quotes")
            for example in misclassified_quotes[:2]:  # Log first 2 examples
                logger.warning(f"    - Pattern '{example['pattern_matched']}' in: {example['quote'][:60]}...")

    # Check 4: Cases referenced in wrapups but missing from summary
    # Detect when agent wrapups mention ServiceNow/Pega cases that weren't retrieved
    # This indicates account linkage issues or query failures
    interactions = payload.get('interactions', [])
    if interactions:
        # Extract case IDs referenced in interaction wrapups
        referenced_cases = {
            'servicenow': set(),
            'pega': set()
        }

        # B-026 FIX: ServiceNow patterns - INC1234567, inc1234567, SN-1234567, INC0123456
        # B-027 FIX: SN- prefix is ServiceNow, not Pega
        servicenow_pattern = re.compile(r'\binc[0-9]{4,}\b|\bsn-[0-9]+\b', re.IGNORECASE)

        # B-027 FIX: Pega patterns - CC-, PEGA-, PZ- formats (ServiceNow uses INC, SN-)
        # Explicitly exclude SN- prefix from Pega pattern to avoid false positives
        pega_pattern = re.compile(r'\b[Pp][Ee][Gg][Aa]-[0-9]+\b|\b[Cc][Cc]-[0-9]+\b|\b[Pp][Zz]-[A-Z0-9]+\b')

        for interaction in interactions:
            wrapup = interaction.get('agent_wrapup_comment', '')
            if wrapup and isinstance(wrapup, str):
                # Check for ServiceNow cases
                sn_matches = servicenow_pattern.findall(wrapup)
                for match in sn_matches:
                    referenced_cases['servicenow'].add(match.upper())

                # Check for Pega cases
                pega_matches = pega_pattern.findall(wrapup)
                for match in pega_matches:
                    referenced_cases['pega'].add(match.upper())

        # Compare referenced cases against what's in the summary
        if referenced_cases['servicenow']:
            # Get ServiceNow case IDs from summary
            summary_sn_cases = summary_json.get('servicenow_cases', [])
            summary_sn_ids = set()
            for case in summary_sn_cases:
                case_id = case.get('case_id', '')
                if case_id:
                    summary_sn_ids.add(case_id.upper())

            # Find missing ServiceNow cases
            missing_sn = referenced_cases['servicenow'] - summary_sn_ids
            if missing_sn:
                warnings.append({
                    'type': 'SERVICENOW_CASES_MISSING',
                    'severity': 'MEDIUM',
                    'description': f'{len(missing_sn)} ServiceNow case(s) referenced in agent wrapups but not found in summary: {", ".join(sorted(missing_sn))}. Case(s) may exist but weren\'t retrieved due to account linkage issues or query failures.',
                    'impact': 'ServiceNow incidents are part of customer history but not visible in summary. This creates incomplete picture of customer issues and may miss critical context for sentiment analysis.',
                    'recommendation': 'Investigate ServiceNow query logic. Verify account linkage (customer_id mapping). Check if cases exist but query filters/JOINs excluded them. Consider adding customer_id to ServiceNow incident table if linkage is missing.',
                    'data_source': 'Customer360_Events.wrapup_comment',
                    'referenced_cases': list(sorted(missing_sn)),
                    'cases_in_summary': len(summary_sn_cases)
                })
                logger.warning(f"  DATA QUALITY WARNING: {len(missing_sn)} ServiceNow case(s) referenced in wrapups but missing from summary: {', '.join(sorted(missing_sn)[:5])}")

        if referenced_cases['pega']:
            # Get Pega case IDs from summary
            summary_pega_cases = summary_json.get('pega_cases', [])
            summary_pega_ids = set()
            for case in summary_pega_cases:
                case_id = case.get('case_id', '')
                if case_id:
                    summary_pega_ids.add(case_id.upper())

            # Find missing Pega cases
            missing_pega = referenced_cases['pega'] - summary_pega_ids
            if missing_pega:
                warnings.append({
                    'type': 'PEGA_CASES_MISSING',
                    'severity': 'MEDIUM',
                    'description': f'{len(missing_pega)} Pega case(s) referenced in agent wrapups but not found in summary: {", ".join(sorted(missing_pega))}. Case(s) may exist but weren\'t retrieved due to account linkage issues or query failures.',
                    'impact': 'Pega cases are part of customer service history but not visible in summary. This creates incomplete picture of customer issues.',
                    'recommendation': 'Investigate Pega query logic. Verify account linkage (customer_id mapping). Check if cases exist but query filters/JOINs excluded them.',
                    'data_source': 'Customer360_Events.wrapup_comment',
                    'referenced_cases': list(sorted(missing_pega)),
                    'cases_in_summary': len(summary_pega_cases)
                })
                logger.warning(f"  DATA QUALITY WARNING: {len(missing_pega)} Pega case(s) referenced in wrapups but missing from summary: {', '.join(sorted(missing_pega)[:5])}")

    # Add warnings to summary_json if any exist
    if warnings:
        # Python validation warnings take precedence over LLM warnings
        # Remove any existing warnings of the same type from LLM
        existing_warnings = summary_json.get('data_quality_warnings', [])
        python_warning_types = {w['type'] for w in warnings}

        # Keep only LLM warnings that don't conflict with Python validation
        filtered_existing = [w for w in existing_warnings if w['type'] not in python_warning_types]

        # Combine: filtered LLM warnings + Python warnings
        all_warnings = filtered_existing + warnings

        summary_json['data_quality_warnings'] = all_warnings
        logger.info(f"  Added {len(warnings)} data quality warning(s) to summary (replacing {len(existing_warnings) - len(filtered_existing)} LLM warnings)")

    # CRITICAL FIX: Ensure Revenue_Cache is listed in agent_briefing.data_sources_used when revenue data is present
    # LLM may omit Revenue_Cache from data_sources_used even when revenue data was used
    customer_profile = payload.get('customer_profile', {})
    revenue_data_present = (
        customer_profile.get('monthly_revenue_total') or
        customer_profile.get('monthly_revenue_mobile') or
        customer_profile.get('monthly_revenue_fixed') or
        customer_profile.get('monthly_revenue_device') or
        customer_profile.get('revenue_cached_at')
    )

    if revenue_data_present:
        agent_briefing = summary_json.get('agent_briefing', {})
        data_sources_used = agent_briefing.get('data_sources_used', '')

        # Check if Revenue_Cache is mentioned
        if 'revenue_cache' not in data_sources_used.lower() and 'revenue' not in data_sources_used.lower():
            # Add Revenue_Cache to data_sources_used
            if data_sources_used:
                updated_sources = f"{data_sources_used}, Revenue_Cache"
            else:
                updated_sources = "Revenue_Cache"

            if 'agent_briefing' in summary_json:
                summary_json['agent_briefing']['data_sources_used'] = updated_sources
                logger.info(f"  Added Revenue_Cache to agent_briefing.data_sources_used: '{updated_sources}'")

    return summary_json


def recalculate_days_open_for_open_issues(summary_json, payload):
    """
    B-003 FIX: Recalculate days_open for open issues at render time.

    Problem: LLM-generated key_issues have frozen days_open_or_resolved values
    calculated at generation time. When summary is viewed days/weeks later,
    these values are stale.

    Solution: For OPEN issues, recalculate days_open using today's date
    and the case's created_date from source data (pega_cases, servicenow_cases).
    """

    key_issues = summary_json.get('key_issues', [])
    if not key_issues:
        return summary_json

    pega_cases = payload.get('pega_cases', [])
    servicenow_cases = payload.get('servicenow_cases', [])

    # Build lookup maps for cases by case_id
    pega_lookup = {str(c.get('case_id', '')): c for c in pega_cases}
    sn_lookup = {str(c.get('case_id', '')): c for c in servicenow_cases}

    updated_count = 0
    today = datetime.now()

    for issue in key_issues:
        status = issue.get('status', '').upper()

        # Only recalculate for OPEN issues (not resolved/closed)
        if status not in ['OPEN', 'IN PROGRESS']:
            continue

        source = issue.get('source', '')

        # Parse source to extract case_id and system
        # Format examples: "Pega case PZ-12345", "ServiceNow INC001234"
        case_id = None
        source_system = None

        if 'Pega case' in source or 'PEGA' in source.upper():
            # Extract Pega case ID
            pega_match = re.search(r'(?:PZ-?[A-Z0-9]+|[A-Z]{2}-[0-9]+)', source, re.IGNORECASE)
            if pega_match:
                case_id = pega_match.group(0).upper()
                source_system = 'pega'
        elif 'ServiceNow' in source or 'INC' in source.upper():
            # Extract ServiceNow case ID (INC followed by digits)
            sn_match = re.search(r'INC[0-9]+', source, re.IGNORECASE)
            if sn_match:
                case_id = sn_match.group(0).upper()
                source_system = 'servicenow'

        if not case_id or not source_system:
            continue

        # Look up the case to get created_date
        case_data = None
        if source_system == 'pega':
            case_data = pega_lookup.get(case_id)
        elif source_system == 'servicenow':
            case_data = sn_lookup.get(case_id)

        if not case_data:
            continue

        # Get created_date and recalculate days_open
        created_date = case_data.get('created_date')
        if not created_date:
            continue

        try:
            # Parse created_date (could be string or datetime)
            if isinstance(created_date, str):
                created = datetime.strptime(created_date[:19], '%Y-%m-%d %H:%M:%S')
            else:
                created = created_date

            # Recalculate days_open
            new_days_open = (today - created).days

            # Update the issue's days_open_or_resolved
            old_days_open = issue.get('days_open_or_resolved', 0)
            if old_days_open != new_days_open:
                issue['days_open_or_resolved'] = new_days_open
                updated_count += 1
                logger.debug(f"  Updated days_open for {source_system} case {case_id}: {old_days_open} -> {new_days_open} days")

        except Exception as e:
            logger.warning(f"  Failed to recalculate days_open for {case_id}: {e}")

    if updated_count > 0:
        logger.info(f"  Recalculated days_open for {updated_count} open issue(s) using today's date")

    return summary_json


# ============================================================
# PROCESS SINGLE CUSTOMER
# ============================================================

def process_customer(conn, customer_id, run_date, watermark=None):
    """Process a single customer for LLM summarization."""

    logger.info(f"Processing customer {customer_id}...")

    # ============================================================
    # STEP 1: Determine scenario (FULL / INCREMENTAL / REBUILD)
    # ============================================================
    # This prevents rebuilding from scratch every time, saving cost and preserving context
    scenario, existing_watermark, existing_summary_json = determine_scenario_and_fetch_summary(
        conn, customer_id, watermark, run_date
    )

    logger.info(f"  Scenario: {scenario}")
    if scenario == 'INCREMENTAL':
        logger.info(f"  Existing watermark: {existing_watermark}")
        logger.info(f"  Merging with previous summary...")

    # ============================================================
    # STEP 2: Build payload based on scenario
    # ============================================================
    if scenario == 'INCREMENTAL':
        # Fetch only NEW events since last summary
        since_timestamp = existing_watermark
    else:
        # FULL or REBUILD: fetch all data
        since_timestamp = None

    payload = build_customer_payload(conn, customer_id, since_timestamp)

    if not payload.get("interactions") and not watermark:
        logger.warning(f"  No interactions found for {customer_id}")
        return None

    # ============================================================
    # STEP 3: Call LLM (scenario-aware)
    # ============================================================
    client = AzureOpenAI(
        api_key=LLM_CONFIG["api_key"],
        api_version=LLM_CONFIG["api_version"],
        azure_endpoint=LLM_CONFIG["azure_endpoint"]
    )

    if scenario == 'INCREMENTAL':
        # Merge existing summary with new data
        llm_result = call_llm_incremental(client, existing_summary_json, payload, run_date)
        summary_text = llm_result['summary_text']
        is_full_build = 0  # Incremental: don't update last_full_build_date
    else:
        # FULL or REBUILD: build from scratch
        llm_result = call_llm_api(client, payload, run_date)
        summary_text = llm_result['summary_text']
        is_full_build = 1  # FULL or REBUILD: update last_full_build_date to today

    # Parse LLM response
    try:
        # Extract JSON from response (handle markdown backticks)
        if "```json" in summary_text:
            summary_text = summary_text.split("```json")[1].split("```")[0].strip()
        elif "```" in summary_text:
            summary_text = summary_text.split("```")[1].split("```")[0].strip()

        summary_json = json.loads(summary_text)
    except Exception as e:
        logger.error(f"  Failed to parse LLM response: {e}")
        logger.debug(f"  Response: {summary_text[:500]}")
        return None

    # CRITICAL FIX: Validate data quality and add warnings
    # Check for pipeline issues where data exists but wasn't processed correctly
    summary_json = validate_data_quality(summary_json, payload)

    # B-003 FIX: Recalculate days_open for open issues using today's date
    # This fixes stale days_open values that were frozen at LLM generation time
    summary_json = recalculate_days_open_for_open_issues(summary_json, payload)

    # CRITICAL FIX: Remove 'channels' field from interaction_summary
    # This field is not in the schema (should be 'contacts_by_type' instead)
    # LLM keeps adding it as null, so we remove it to clean up the JSON
    if 'interaction_summary' in summary_json and 'channels' in summary_json['interaction_summary']:
        del summary_json['interaction_summary']['channels']
        logger.info(f"  Removed 'channels' field from interaction_summary (not in schema, use 'contacts_by_type' instead)")

    # CRITICAL FIX: Override LLM's interaction_summary.total_contacts with actual payload count
    # The LLM generates its own count which may be wrong. Use Python's actual count from payload.
    interactions = payload.get('interactions', [])
    actual_contact_count = len(interactions)

    if 'interaction_summary' in summary_json and 'total_contacts' in summary_json['interaction_summary']:
        llm_count = summary_json['interaction_summary']['total_contacts']
        if llm_count != actual_contact_count:
            logger.warning(f"  Correcting interaction_summary.total_contacts: LLM said {llm_count}, actual is {actual_contact_count}")
            summary_json['interaction_summary']['total_contacts'] = actual_contact_count

    # CRITICAL FIX: Build contacts_by_type from actual interaction data to ensure it sums to total_contacts
    # LLM may miss some contact types, causing the sum to not equal total_contacts
    if interactions and 'interaction_summary' in summary_json:
        contacts_by_type = {}
        for interaction in interactions:
            # CRITICAL FIX: Filter out None/null values, default to "Unknown" only if genuinely missing
            contact_type_raw = interaction.get('contact_type') or interaction.get('channel')
            # Only default to "Unknown" if contact_type_raw is explicitly None or empty string, not if it's "null" string
            if contact_type_raw is None or contact_type_raw == '':
                contact_type = 'Unknown'
            elif contact_type_raw and contact_type_raw.lower() != 'null':
                contact_type = str(contact_type_raw)
            else:
                # Skip "null" string values
                continue

            # CRITICAL FIX: Filter out invalid contact types that are actually issue categories
            # Some data has issue categories like "Can't Connect to National Network" or "Cancel Order"
            # These are NOT contact channels and should be excluded from contacts_by_type
            invalid_contact_patterns = [
                "can't connect", "cannot connect", "network issue", "no service",
                "cancel order", "cancellation", "billing issue", "payment issue",
                "complaint", "dispute", "porting", "migration"
            ]

            # B-030 FIX: Only allow valid contact channel enum values
            # ServiceNow incident titles should NOT appear as channel keys
            allowed_contact_channels = {
                'phone', 'web', 'ivr', 'inbound', 'chat', 'outbound',
                'email', 'sms', 'whatsapp', 'social media', 'store', 'other'
            }

            contact_type_lower = contact_type.lower()

            # First check if it's in the allowed channels
            if contact_type_lower not in allowed_contact_channels:
                logger.warning(f"  B-030 FIX: Skipping invalid contact_type '{contact_type}' (not in allowed channel enum)")
                continue

            # Then check for invalid patterns (defense-in-depth)
            if any(pattern in contact_type_lower for pattern in invalid_contact_patterns):
                logger.warning(f"  Skipping invalid contact_type '{contact_type}' (appears to be issue category, not channel)")
                continue

            if contact_type:
                contacts_by_type[contact_type] = contacts_by_type.get(contact_type, 0) + 1

        # CRITICAL FIX: Always filter out invalid contact types from LLM's contacts_by_type
        # Even if the sum matches, we need to remove invalid entries (issue categories masquerading as channels)
        llm_contacts_by_type = summary_json['interaction_summary'].get('contacts_by_type', {})

        # B-030 FIX: Filter to allowed contact channels only
        allowed_contact_channels = {
            'phone', 'web', 'ivr', 'inbound', 'chat', 'outbound',
            'email', 'sms', 'whatsapp', 'social media', 'store', 'other'
        }

        # Filter out invalid types from LLM's response
        invalid_contact_patterns = [
            "can't connect", "cannot connect", "network issue", "no service",
            "cancel order", "cancellation", "billing issue", "payment issue",
            "complaint", "dispute", "porting", "migration"
        ]

        filtered_llm_contacts = {}
        for contact_type, count in llm_contacts_by_type.items():
            contact_type_lower = contact_type.lower()

            # Check if it's an allowed channel
            if contact_type_lower not in allowed_contact_channels:
                logger.warning(f"  B-030 FIX: Filtering invalid contact_type '{contact_type}' from LLM (not in allowed channels, count: {count})")
                continue

            # Also check for invalid patterns
            if any(pattern in contact_type_lower for pattern in invalid_contact_patterns):
                logger.warning(f"  Filtering invalid contact_type '{contact_type}' from LLM response (count: {count})")
            else:
                filtered_llm_contacts[contact_type] = count

        llm_sum = sum(filtered_llm_contacts.values()) if filtered_llm_contacts else 0

        # Update if sum doesn't match OR if we filtered out invalid types
        if llm_sum != actual_contact_count or len(filtered_llm_contacts) != len(llm_contacts_by_type):
            if len(filtered_llm_contacts) != len(llm_contacts_by_type):
                logger.warning(f"  Filtered {len(llm_contacts_by_type) - len(filtered_llm_contacts)} invalid contact type(s) from LLM response")
            if llm_sum != actual_contact_count:
                logger.warning(f"  Correcting contacts_by_type: filtered sum was {llm_sum}, actual is {actual_contact_count}")

            # Use Python-built contacts_by_type if counts don't match, otherwise use filtered LLM data
            if llm_sum != actual_contact_count:
                summary_json['interaction_summary']['contacts_by_type'] = contacts_by_type
                logger.info(f"  Updated contacts_by_type: {contacts_by_type}")
            else:
                summary_json['interaction_summary']['contacts_by_type'] = filtered_llm_contacts
                logger.info(f"  Updated contacts_by_type (filtered): {filtered_llm_contacts}")

        # ============================================================
        # B7-M1 FIX: contacts_by_type={} empty despite documented interactions
        # ============================================================
        # When contacts_by_type is empty but total_contacts>0, the aggregation step failed.
        # Use input_data_summary.contact_types as canonical source and raise DQW.

        interaction_summary = summary_json.get('interaction_summary', {})
        contacts_by_type = interaction_summary.get('contacts_by_type', {})
        total_contacts = interaction_summary.get('total_contacts', 0)

        # Check if contacts_by_type is empty despite having contacts
        if total_contacts > 0 and not contacts_by_type:
            logger.warning(f"  B7-M1 FIX: contacts_by_type is empty despite {total_contacts} interactions documented")

            # Try to get canonical source from input_data_summary.contact_types
            input_contacts = summary_json.get('input_data_summary', {}).get('time_period', {}).get('contact_types', {})

            if input_contacts:
                # B7-M1 FIX: Use input_data_summary.contact_types as canonical source
                summary_json['interaction_summary']['contacts_by_type'] = dict(input_contacts)
                logger.info(f"  B7-M1 FIX: Copied contacts_by_type from input_data_summary: {input_contacts}")
            else:
                # B7-M1 FIX: Raise DQW when no canonical source available
                dq_warnings = summary_json.get('data_quality_warnings', [])
                dq_warnings.append({
                    'type': 'CONTACTS_BY_TYPE_MISSING',
                    'severity': 'MEDIUM',
                    'description': f'interaction_summary.contacts_by_type is empty despite {total_contacts} interactions documented. Aggregation step failed and input_data_summary.contact_types also unavailable.',
                    'recommendation': 'Manually verify interaction channel data. Check if contact_type field is populated in interaction records.',
                    'impact': 'Agents cannot see interaction channel breakdown for this customer.'
                })
                summary_json['data_quality_warnings'] = dq_warnings
                logger.warning(f"  B7-M1 FIX: Raised DQW - CONTACTS_BY_TYPE_MISSING ({total_contacts} interactions, no breakdown available)")

        # ============================================================
        # B7-M7 FIX: contacts_by_type mismatch within same record
        # ============================================================
        # Validate consistency between interaction_summary.contacts_by_type and input_data_summary.contact_types
        # Use input_data_summary as canonical source to ensure consistency

        input_contacts = summary_json.get('input_data_summary', {}).get('time_period', {}).get('contact_types', {})
        interaction_contacts = summary_json.get('interaction_summary', {}).get('contacts_by_type', {})

        if input_contacts and interaction_contacts:
            input_sum = sum(input_contacts.values())
            interaction_sum = sum(interaction_contacts.values())

            # Check for mismatch
            if input_sum != interaction_sum:
                logger.warning(f"  B7-M7 FIX: contacts_by_type mismatch! input_data_summary sum={input_sum}, interaction_summary sum={interaction_sum}")
                logger.warning(f"  B7-M7 FIX: input_data_summary.contact_types={input_contacts}")
                logger.warning(f"  B7-M7 FIX: interaction_summary.contacts_by_type={interaction_contacts}")

                # B7-M7 FIX: Use input_data_summary as canonical source
                summary_json['interaction_summary']['contacts_by_type'] = dict(input_contacts)
                logger.info(f"  B7-M7 FIX: Synced contacts_by_type from input_data_summary to interaction_summary (canonical source)")

    # B-034 FIX: Validate engagement_style/data_confidence consistency
    # engagement_style='Silent' must have data_confidence='LOW' (Silent means no data)
    interaction_summary = summary_json.get('interaction_summary', {})
    engagement_style = interaction_summary.get('engagement_style', '').strip()
    data_confidence = summary_json.get('sentiment_analysis', {}).get('data_confidence', '').strip()

    if engagement_style and data_confidence:
        engagement_lower = engagement_style.lower()
        confidence_lower = data_confidence.lower()

        # Check for contradictions
        if engagement_lower == 'silent' and confidence_lower == 'high':
            # Silent means no data, can't have HIGH confidence
            summary_json['sentiment_analysis']['data_confidence'] = 'LOW'
            logger.info(f"  B-034 FIX: Corrected data_confidence from HIGH to LOW (engagement_style='Silent' means no data available)")
        elif engagement_lower in ['highly engaged', 'engaged'] and confidence_lower == 'low':
            # Highly engaged but low confidence - potential contradiction
            logger.warning(f"  B-034 FIX: Potential contradiction - engagement_style='{engagement_style}' but data_confidence='LOW'. Verify data availability.")
        elif engagement_lower == 'silent' and not summary_json.get('sentiment_analysis', {}).get('reasoning'):
            # Silent with no reasoning - add explanation
            summary_json['sentiment_analysis']['reasoning'] = (
                "Insufficient interaction data to determine sentiment. "
                "Customer has limited engagement history (Silent engagement style). "
                "Sentiment analysis based on available data only."
            )
            logger.info(f"  B-034 FIX: Added reasoning for Silent engagement_style (no data available)")

    # CRITICAL FIX: Populate call_recordings_summary with actual data when LLM omits it
    # High-frustration customers with unanalyzed call recordings are a missed signal opportunity
    call_recordings = payload.get('call_recordings', [])
    if call_recordings:
        quotes_count = 0
        for r in call_recordings:
            quotes = r.get('customer_quotes')
            if quotes and isinstance(quotes, list):
                quotes_count += len(quotes)

        actual_calls = len(call_recordings)

        # Check if LLM populated the field or left it as None
        llm_summary = summary_json.get('call_recordings_summary', {})
        if llm_summary.get('total_calls') is None or llm_summary.get('quotes_extracted') is None:
            logger.info(f"  Populating call_recordings_summary: {actual_calls} calls, {quotes_count} quotes extracted")
            summary_json['call_recordings_summary'] = {
                'total_calls': actual_calls,
                'quotes_extracted': quotes_count,
                'data_source': 'CallTranscript.customer_quotes_json'
            }

    # Apply explainable gating
    summary_json = apply_explainable_gating(summary_json, payload)

    # CRITICAL FIX: Add customer_profile to summary_json
    # The customer_profile from Revenue_Cache contains critical data including customer_id
    if payload.get('customer_profile'):
        summary_json['customer_profile'] = payload['customer_profile']

    # ============================================================
    # STEP 4: Generate Dashboard Metrics with Reasoning
    # ============================================================
    # Generate dashboard metrics (health_score, churn_risk, effort_score, etc.)
    # Hybrid approach: Python calculates scores → LLM generates natural language explanations
    logger.info(f"  Generating dashboard metrics for {customer_id}")
    dashboard_metrics = generate_dashboard_metrics(summary_json, payload)

    # Generate natural language reasoning for each metric using LLM
    dashboard_metrics = generate_dashboard_metrics_reasoning(client, dashboard_metrics, payload, summary_json)

    # Add to summary_json
    summary_json['dashboard_metrics'] = dashboard_metrics
    logger.info(f"  Dashboard metrics generated: health={dashboard_metrics['health_score']['value']}, churn_risk={dashboard_metrics['churn_risk']['score']}")

    # CRITICAL FIX: Copy escalation_risk from dashboard_metrics to root level
    # The schema has escalation_risk at root level, but LLM leaves it as "Pending"
    # Python calculates it in dashboard_metrics, so we need to copy it to root
    if 'escalation_risk' in dashboard_metrics:
        escalation_value = dashboard_metrics['escalation_risk']['value']
        escalation_label = dashboard_metrics['escalation_risk']['label']
        summary_json['escalation_risk'] = escalation_label
        logger.info(f"  Set escalation_risk to: {escalation_label} (score: {escalation_value})")

        # B7-M9 FIX: Validate consistency with threatened_escalation
        # If threatened_escalation=true in escalation_threats, escalation_risk must not be 'Low'
        escalation_threats = summary_json.get('threat_indicators', {}).get('escalation_threats', {})
        threatened_escalation = escalation_threats.get('threatened_escalation', False)

        if threatened_escalation and escalation_label == 'Low':
            # B7-M9 FIX: Escalation threatened but risk is Low - inconsistent!
            logger.warning(f"  B7-M9 FIX: Inconsistency detected! threatened_escalation=true but escalation_risk='{escalation_label}'. Forcing to 'Medium'.")

            # B7-M9 FIX: Determine appropriate level based on escalation_target
            escalation_target = escalation_threats.get('escalation_target', '').lower()

            # CEO/Director/legal escalations must be High
            if escalation_target in ['ceo', 'chief executive', 'director', 'vp', 'vice president', 'legal', 'regulator', 'comreg']:
                escalation_label = 'High'
                escalation_value = 2.0
            elif escalation_target in ['manager', 'team lead', 'supervisor']:
                escalation_label = 'Medium'
                escalation_value = 1.0
            else:
                escalation_label = 'Medium'
                escalation_value = 1.0

            # Update both root level and dashboard_metrics
            summary_json['escalation_risk'] = escalation_label
            dashboard_metrics['escalation_risk']['label'] = escalation_label
            dashboard_metrics['escalation_risk']['value'] = escalation_value
            logger.info(f"  B7-M9 FIX: Corrected escalation_risk to: {escalation_label} (score: {escalation_value})")

    # B-001 FIX: Recalculate health_score based on key_issues priority
    # The initial health_score only considered Pega/ServiceNow cases, not LLM-detected key_issues
    # If key_issues have CRITICAL/HIGH priority, health_score should be recalculated
    key_issues = summary_json.get('key_issues', [])
    issue_resolution_actions = summary_json.get('recommended_actions', {}).get('issue_resolution_actions', [])

    # Count CRITICAL and HIGH priority from key_issues and issue_resolution_actions
    critical_from_key_issues = 0
    high_from_key_issues = 0

    for issue in key_issues:
        status = issue.get('status', '').upper()
        priority = issue.get('priority', '').upper()
        # Only count open issues (not Resolved/Closed)
        if status not in ['RESOLVED', 'CLOSED']:
            if priority == 'CRITICAL':
                critical_from_key_issues += 1
            elif priority == 'HIGH':
                high_from_key_issues += 1

    for action in issue_resolution_actions:
        status = action.get('status', '').upper()
        priority = action.get('priority', '').upper()
        # Only count open actions (not Resolved/Closed)
        if status not in ['RESOLVED', 'CLOSED']:
            if priority == 'CRITICAL':
                critical_from_key_issues += 1
            elif priority == 'HIGH':
                high_from_key_issues += 1

    if critical_from_key_issues > 0 or high_from_key_issues > 0:
        # B-001 FIX: Recalculate health_score with key_issues priority
        logger.info(f"  B-001 FIX: Recalculating health_score with {critical_from_key_issues} CRITICAL and {high_from_key_issues} HIGH key_issues")

        # Get original health_score parameters
        sentiment_analysis = summary_json.get('sentiment_analysis', {})
        frustration_score = sentiment_analysis.get('frustration_score', 0)

        # FIX: Get customer_profile from payload (not from undefined local variable)
        customer_profile = payload.get('customer_profile', {})

        # Recalculate health_score with updated counts
        mobile_active = customer_profile.get('mobile_active', True) if customer_profile else True
        fixed_active = customer_profile.get('fixed_active', True) if customer_profile else True
        # B12 FIX: subscription flags so single-service customers are not penalised
        has_mobile = customer_profile.get('has_mobile', True) if customer_profile else True
        has_fixed  = customer_profile.get('has_fixed',  True) if customer_profile else True

        recalculated_health = calculate_health_score(
            frustration_score,
            0,  # open_cases not used for this recalc
            0,  # total_contacts not used for this recalc
            payload.get('customer_quotes_analysis'),
            mobile_active,
            fixed_active,
            critical_from_key_issues,
            high_from_key_issues,
            has_mobile_service=has_mobile,
            has_fixed_service=has_fixed
        )

        original_health = dashboard_metrics['health_score']['value']

        # Only update if recalculated score is lower (worse health)
        if recalculated_health < original_health:
            logger.warning(f"  B-001 FIX: Health score reduced from {original_health} to {recalculated_health} due to {critical_from_key_issues} CRITICAL + {high_from_key_issues} HIGH key_issues")

            # Update dashboard_metrics
            dashboard_metrics['health_score']['value'] = recalculated_health
            dashboard_metrics['health_score']['label'] = get_health_score_label(recalculated_health)
            dashboard_metrics['health_score']['color'] = get_health_score_color(recalculated_health)

            # BUG FIX #2 & #3: Regenerate health score reasoning to match new value
            # The old reasoning was based on the original health score, need to update it
            old_reasoning = dashboard_metrics['health_score'].get('reasoning', '')
            if old_reasoning and f'{original_health}' in old_reasoning:
                # Replace old score value with new score value in reasoning
                new_reasoning = re.sub(rf'\b{original_health}/100', f'{recalculated_health}/100', old_reasoning)
                new_reasoning = re.sub(r'\blow health score \(?\d+/\d+\)?', f'health score ({recalculated_health}/100)', new_reasoning, flags=re.IGNORECASE)
                new_reasoning = re.sub(r'\bhealth score of \d+/\d+', f'health score of {recalculated_health}/100', new_reasoning, flags=re.IGNORECASE)
                dashboard_metrics['health_score']['reasoning'] = new_reasoning
                logger.info(f"  BUG FIX #2: Updated health score reasoning from {original_health} to {recalculated_health}")

            # Check for boilerplate/fallback reasoning that doesn't match actual health status
            health_label = dashboard_metrics['health_score']['label']
            if health_label in ['Warning', 'At Risk', 'Critical']:
                # Non-healthy status should not have "within normal range" boilerplate
                if old_reasoning and ('within normal range' in old_reasoning.lower() or 'no immediate concerns' in old_reasoning.lower()):
                    # Replace with appropriate reasoning based on actual status
                    if health_label == 'Warning':
                        new_reasoning = f"Customer health score is {recalculated_health}/100 ({health_label}). Warning indicators detected - attention needed to prevent escalation."
                    elif health_label == 'At Risk':
                        new_reasoning = f"Customer health score is {recalculated_health}/100 ({health_label}). Significant concerns exist - immediate intervention recommended."
                    elif health_label == 'Critical':
                        new_reasoning = f"Customer health score is {recalculated_health}/100 ({health_label}). Critical issues detected - urgent action required."
                    else:
                        new_reasoning = f"Customer health score is {recalculated_health}/100 ({health_label})."
                    dashboard_metrics['health_score']['reasoning'] = new_reasoning
                    logger.info(f"  BUG FIX #3: Replaced boilerplate health reasoning with appropriate {health_label} status reasoning")

            # BUG FIX #8: Check for stale template placeholders like (N), {N}, (score), etc.
            # These indicate template variables that were never replaced with actual values
            old_reasoning = dashboard_metrics['health_score'].get('reasoning', '')
            if old_reasoning:
                # Common placeholder patterns that indicate stale templates
                placeholder_patterns = [r'\(N\)', r'\{N\}', r'\(score\)', r'\{score\}', r'\(value\)', r'\{value\}',
                                       r'\(health\)', r'\{health\}', r'\(\d*\)', r'\{\d*\}']
                has_placeholder = False
                for pattern in placeholder_patterns:
                    if re.search(pattern, old_reasoning):
                        has_placeholder = True
                        break

                if has_placeholder:
                    # Replace all placeholders with actual values
                    actual_health = dashboard_metrics['health_score']['value']
                    actual_label = dashboard_metrics['health_score']['label']
                    # Remove placeholders and insert actual values
                    new_reasoning = re.sub(r'\(N\)|\{N\}|\(score\)|\{score\}', str(actual_health), old_reasoning)
                    new_reasoning = re.sub(r'\(value\)|\{value\}|\(health\)|\{health\}', str(actual_health), new_reasoning)
                    # Remove any remaining empty placeholders like () or {}
                    new_reasoning = re.sub(r'\(\)|\{\}', '', new_reasoning)
                    # If reasoning still looks broken, replace entirely
                    if '(N)' in new_reasoning or '{N}' in new_reasoning or 'Good' in new_reasoning and actual_label != 'Healthy':
                        new_reasoning = f"Customer health score is {actual_health}/100 ({actual_label})."
                    dashboard_metrics['health_score']['reasoning'] = new_reasoning
                    logger.warning(f"  BUG FIX #8: Fixed stale template placeholders in health_score reasoning: {old_reasoning[:60]}... → {new_reasoning[:60]}...")

    # CRITICAL FIX: Align retention_risk_signals.churn_probability with dashboard_metrics.churn_risk
    # This prevents contradictions where LLM says "Medium" but Python calculates "Very Low"
    if 'retention_risk_signals' in summary_json:
        calculated_churn_prob = dashboard_metrics.get('churn_risk', {}).get('probability')
        if calculated_churn_prob:
            summary_json['retention_risk_signals']['churn_probability'] = calculated_churn_prob
            logger.info(f"  Synchronized retention_risk_signals.churn_probability to: {calculated_churn_prob}")

    # BUG FIX #10: Synchronize predictive_insights.churn_risk_indicator with churn_risk.score
    # LLM generates churn_risk_indicator ('Elevated', 'Normal', 'Low') independently from Python-calculated churn_risk.score
    # This creates contradictions when LLM says 'Elevated' but score is 26 (Low)
    if 'predictive_insights' in summary_json:
        churn_score = dashboard_metrics.get('churn_risk', {}).get('score', 0)
        llm_indicator = summary_json['predictive_insights'].get('churn_risk_indicator', '')

        # BUG FIX #6 & #12: Map score to indicator using standard vocabulary
        # Standard values: {Critical, Elevated, High, Normal} - 'Low' is not standard
        # Critical>=70, Elevated>=50, High>=30, Normal<30
        if churn_score >= 70:
            expected_indicator = 'Critical'
        elif churn_score >= 50:
            expected_indicator = 'Elevated'
        elif churn_score >= 30:
            expected_indicator = 'High'
        else:
            expected_indicator = 'Normal'

        # If LLM indicator contradicts calculated score, override it
        if llm_indicator != expected_indicator:
            summary_json['predictive_insights']['churn_risk_indicator'] = expected_indicator
            logger.info(f"  BUG FIX #10: Synchronized churn_risk_indicator from '{llm_indicator}' to '{expected_indicator}' (score={churn_score})")

        # CRITICAL FIX: Ensure products_at_risk includes "Device" when customer has device revenue
        # LLM may only flag "Mobile" even when customer has significant device revenue at risk
        customer_profile = summary_json.get('customer_profile', {})
        monthly_device_revenue = customer_profile.get('monthly_revenue_device', 0) or customer_profile.get('device_financing_revenue', 0) or 0
        device_count = customer_profile.get('device_count', 0) or 0

        products_at_risk = summary_json['retention_risk_signals'].get('products_at_risk', [])

        # Add "Device" to products_at_risk if there's device revenue or devices
        if (monthly_device_revenue > 0 or device_count > 0) and 'Device' not in products_at_risk:
            if products_at_risk:
                summary_json['retention_risk_signals']['products_at_risk'] = products_at_risk + ['Device']
            else:
                summary_json['retention_risk_signals']['products_at_risk'] = ['Device']
            logger.info(f"  Added 'Device' to products_at_risk (€{monthly_device_revenue}/month device revenue, {device_count} devices)")

        # CRITICAL FIX: Override incorrect churn_risk_magnification boilerplate
        # LLM may say "Single SIM risk" when customer has no SIMs, is fully churned, or has multiple SIMs
        sim_count = customer_profile.get('plan_count')  # Use plan_count
        mobile_active = customer_profile.get('mobile_active')
        fixed_active = customer_profile.get('fixed_active')
        churn_magnification = summary_json['retention_risk_signals'].get('churn_risk_magnification', '')

        # Check for incorrect "Single SIM risk" label
        if churn_magnification and 'single sim' in churn_magnification.lower():
            if sim_count is None or sim_count == 0 or mobile_active is False:
                # No SIMs or inactive — override
                if fixed_active:
                    summary_json['retention_risk_signals']['churn_risk_magnification'] = "Fixed-line service only"
                else:
                    summary_json['retention_risk_signals']['churn_risk_magnification'] = None
                logger.info(f"  Overrode churn_risk_magnification: removed 'Single SIM risk' for customer with sim_count={sim_count}, mobile_active={mobile_active}")
            elif sim_count and sim_count > 1:
                # Multiple SIMs — should say Multi-SIM, not Single SIM
                summary_json['retention_risk_signals']['churn_risk_magnification'] = "Multi-SIM risk"
                logger.info(f"  Overrode churn_risk_magnification: 'Single SIM risk' → 'Multi-SIM risk' for sim_count={sim_count}")

        # B_VAR FIX: Sync value_at_risk.churn_risk_magnification from retention_risk_signals.
        # Root cause: retention_risk_signals.churn_risk_magnification is corrected above
        # (Single→Multi-SIM, or cleared), but value_at_risk.churn_risk_magnification is never
        # updated.  Result: two different magnification labels coexist in the same record.
        corrected_magnification = summary_json['retention_risk_signals'].get('churn_risk_magnification')
        if 'value_at_risk' not in summary_json:
            summary_json['value_at_risk'] = {}
        var_magnification = summary_json['value_at_risk'].get('churn_risk_magnification')
        if var_magnification != corrected_magnification:
            summary_json['value_at_risk']['churn_risk_magnification'] = corrected_magnification
            logger.info(f"  B_VAR FIX: Synced value_at_risk.churn_risk_magnification: '{var_magnification}' → '{corrected_magnification}'")

        # CRITICAL FIX: Override retention_priority for never-activated customers
        # Customers with tenure_months=0 and no revenue never activated service, so retention_priority should be false
        # There's nothing to "retain" for a customer who never started using the service
        current_retention_priority = summary_json['retention_risk_signals'].get('retention_priority')
        calculated_churn_prob = dashboard_metrics.get('churn_risk', {}).get('probability')
        tenure_months = customer_profile.get('tenure_months', 0)
        monthly_revenue = customer_profile.get('monthly_revenue_total', 0)

        # CRITICAL FIX: Set retention_priority based on churn_probability
        # Medium or higher churn risk should trigger retention_priority=true
        churn_priority_mapping = {
            'Very High': True,
            'High': True,
            'Medium': True,
            'Low': False,
            'Very Low': False
        }

        if calculated_churn_prob and calculated_churn_prob in churn_priority_mapping:
            expected_priority = churn_priority_mapping[calculated_churn_prob]
            if current_retention_priority != expected_priority:
                summary_json['retention_risk_signals']['retention_priority'] = expected_priority
                logger.info(f"  Set retention_priority to {expected_priority} based on churn_probability={calculated_churn_prob}")

        # B-033 FIX: Never-activated or cancelled customer override (takes precedence)
        # retention_priority should be false when there's nothing to retain (no active service, no revenue, no tenure)
        mobile_active = customer_profile.get('mobile_active', True)
        fixed_active = customer_profile.get('fixed_active', True)
        is_inactive = not mobile_active and not fixed_active

        if current_retention_priority is True and (tenure_months == 0 or (is_inactive and (monthly_revenue is None or monthly_revenue == 0))):
            summary_json['retention_risk_signals']['retention_priority'] = False
            summary_json['retention_risk_signals']['recommended_action'] = 'No action required - customer has no active service or revenue to retain'
            logger.info(f"  B-033 FIX: Overrode retention_priority to False: inactive customer (tenure_months={tenure_months}, mobile_active={mobile_active}, fixed_active={fixed_active}, revenue={monthly_revenue})")

        # B7-C1 FIX: Enforce boolean type for retention_priority (prevent empty string '')
        # LLM may leave field as blank string '' - must be converted to boolean False
        # Downstream systems require strict boolean type for this field
        if 'retention_risk_signals' in summary_json:
            rp_value = summary_json['retention_risk_signals'].get('retention_priority')
            if rp_value == '' or rp_value is None or rp_value == 'Calculated by Python - leave blank':
                summary_json['retention_risk_signals']['retention_priority'] = False
                logger.warning(f"  B7-C1 FIX: Corrected retention_priority from '{rp_value}' to False (empty string/null to boolean)")

        # B7-C1 FIX: Always sync value_at_risk.retention_priority from retention_risk_signals
        # This ensures both fields are boolean and consistent
        rr_priority = summary_json.get('retention_risk_signals', {}).get('retention_priority', False)

        # Ensure value_at_risk section exists
        if 'value_at_risk' not in summary_json:
            summary_json['value_at_risk'] = {}

        # Sync retention_priority to value_at_risk
        var_value = summary_json['value_at_risk'].get('retention_priority')
        if var_value != rr_priority or var_value == '' or var_value is None or not isinstance(var_value, bool):
            summary_json['value_at_risk']['retention_priority'] = bool(rr_priority)
            logger.info(f"  B7-C1 FIX: Synced value_at_risk.retention_priority to {bool(rr_priority)} (was: '{var_value}')")

        # CRITICAL FIX: Add DQW for marketing_consent=null (unknown consent status)
        # GDPR requires consent=True for marketing. consent=None should be flagged as treat-as-no-consent
        marketing_consent = customer_profile.get('marketing_consent')
        if marketing_consent is None:
            # Check if DQW already exists for this
            dq_warnings = summary_json.get('data_quality_warnings', [])
            consent_warning_exists = any('marketing_consent' in w.get('description', '') for w in dq_warnings)

            if not consent_warning_exists:
                dq_warnings.append({
                    'type': 'consent_status_unknown',
                    'severity': 'HIGH',
                    'description': 'Customer marketing_consent is null (unknown). GDPR requires verified consent for marketing outreach. Treat as no-consent until verified.',
                    'recommendation': 'Verify marketing consent status before any proactive outreach. Customer should be treated as opt-out until consent is confirmed.',
                    'impact': 'Marketing contacts blocked due to unknown consent status. May miss legitimate retention opportunities.'
                })
                summary_json['data_quality_warnings'] = dq_warnings
                logger.warning(f"  Added DQW: marketing_consent=null - treat as no-consent until verified")

        # CRITICAL FIX: Flag tenure_months anomaly for high-value customers
        # Customers with high revenue but very low tenure (1-2 months) may have data migration issues
        # where tenure was reset during account consolidation
        total_contacts = summary_json.get('interaction_summary', {}).get('total_contacts', 0)

        if (tenure_months <= 2 and
            monthly_revenue and monthly_revenue >= 100 and
            total_contacts >= 10):
            # High value customer with suspiciously low tenure and high contact volume
            dq_warnings = summary_json.get('data_quality_warnings', [])
            tenure_warning_exists = any('tenure_months' in w.get('description', '') for w in dq_warnings)

            if not tenure_warning_exists:
                dq_warnings.append({
                    'type': 'tenure_data_anomaly',
                    'severity': 'MEDIUM',
                    'description': f'Customer has High Value revenue (€{monthly_revenue:.0f}/month) with {total_contacts} contacts but tenure_months={tenure_months}. This may indicate a data migration issue where tenure was reset during account consolidation.',
                    'recommendation': 'Verify tenure_months accuracy. Check if this is a migrated/consolidated account where original tenure was lost. Consider cross-referencing billing system start date.',
                    'current_values': {
                        'tenure_months': tenure_months,
                        'monthly_revenue': monthly_revenue,
                        'total_contacts': total_contacts
                    }
                })
                summary_json['data_quality_warnings'] = dq_warnings
                logger.warning(f"  Added DQW: tenure_months={tenure_months} anomaly for high-value customer (€{monthly_revenue:.0f}/month, {total_contacts} contacts)")

    # Also align value_at_risk.churn_probability if it exists
    if 'value_at_risk' in summary_json:
        calculated_churn_prob = dashboard_metrics.get('churn_risk', {}).get('probability')
        if calculated_churn_prob:
            summary_json['value_at_risk']['churn_probability'] = calculated_churn_prob

    # ============================================================
    # VALIDATION CHECKS - Ensure internal consistency
    # ============================================================

    # BUG FIX #5: Validate escalation_risk consistency between human_briefing and dashboard_metrics
    human_briefing = summary_json.get('human_briefing', '')
    dashboard_escalation_label = dashboard_metrics.get('escalation_risk', {}).get('label', '')
    dashboard_escalation_value = dashboard_metrics.get('escalation_risk', {}).get('value', 0)

    # Extract escalation risk from human_briefing (LLM-generated)
    if human_briefing:
        human_escalation_match = None
        if 'escalation risk: high' in human_briefing.lower():
            human_escalation_match = 'High'
        elif 'escalation risk: medium' in human_briefing.lower():
            human_escalation_match = 'Medium'
        elif 'escalation risk: low' in human_briefing.lower():
            human_escalation_match = 'Low'

        # Check for contradiction and add DQW if found
        if human_escalation_match and human_escalation_match.lower() != dashboard_escalation_label.lower():
            dq_warnings = summary_json.get('data_quality_warnings', [])

            # Check if this specific contradiction already exists
            contradiction_exists = any(
                d.get('type') == 'escalation_risk_contradiction' and
                d.get('human_briefing_risk') == human_escalation_match
                for d in dq_warnings
            )

            if not contradiction_exists:
                dq_warnings.append({
                    'type': 'escalation_risk_contradiction',
                    'severity': 'MEDIUM',
                    'description': f"Contradictory escalation risk signals: human_briefing says '{human_escalation_match}' but dashboard_metrics says '{dashboard_escalation_label}' (score: {dashboard_escalation_value}).",
                    'impact': "Agent receives conflicting escalation signals. Python-calculated risk (based on threat_indicators) takes precedence for consistency.",
                    'recommendation': f"Trust the dashboard_metrics value: {dashboard_escalation_label}. The human_briefing was LLM-generated independently and may not have considered all threat_indicators."
                })
                summary_json['data_quality_warnings'] = dq_warnings
                logger.warning(f"  BUG FIX #5: Added DQW - Escalation risk contradiction: human_briefing={human_escalation_match}, dashboard={dashboard_escalation_label}")

    # BUG FIX #6: Validate retention_priority vs recommended_action consistency
    recommended_actions = summary_json.get('recommended_actions', {})
    retention_priority = summary_json.get('value_at_risk', {}).get('retention_priority')
    priority_focus = recommended_actions.get('priority_focus', '')

    # Check for contradiction: retention_priority=False but action says "Immediate retention"
    if retention_priority is False and priority_focus:
        action_text = str(priority_focus).lower()
        if 'retention' in action_text or 'immediate' in action_text or 'urgent' in action_text:
            dq_warnings = summary_json.get('data_quality_warnings', [])

            # Check if DQW already exists
            retention_dqw_exists = any(d.get('type') == 'retention_priority_contradiction' for d in dq_warnings)

            if not retention_dqw_exists:
                dq_warnings.append({
                    'type': 'retention_priority_contradiction',
                    'severity': 'HIGH',
                    'description': f"Contradictory retention signals: retention_priority=False (don't prioritize) but priority_focus='{priority_focus}' (immediate action required).",
                    'impact': "Agent receives conflicting retention instructions. value_at_risk.retention_priority=False says skip retention, but recommended_actions.priority_focus says immediate action.",
                    'recommendation': f"Clarify which signal is correct. If customer needs immediate retention, set retention_priority=True. If not, update priority_focus to 'STANDARD' or 'OPPORTUNITY'."
                })
                summary_json['data_quality_warnings'] = dq_warnings
                logger.warning(f"  BUG FIX #6: Added DQW - Retention priority contradiction: retention_priority=False but priority_focus={priority_focus}")

    # BUG FIX #7: Validate threatened_cancellation vs churn_probability
    # If customer has already cancelled (threatened_cancellation=true), churn probability should be HIGH
    threat_indicators = summary_json.get('threat_indicators', {})
    escalation_threats = threat_indicators.get('escalation_threats', {})
    threatened_cancellation = escalation_threats.get('threatened_cancellation', False)
    churn_probability = dashboard_metrics.get('churn_risk', {}).get('probability', '').lower()
    churn_score = dashboard_metrics.get('churn_risk', {}).get('score', 0)

    if threatened_cancellation:
        # Customer has explicitly cancelled - this is MAXIMUM churn risk
        # Check if churn_probability reflects this reality
        if churn_probability in ['very low', 'low']:
            dq_warnings = summary_json.get('data_quality_warnings', [])

            # Check if DQW already exists
            cancellation_dqw_exists = any(
                d.get('type') == 'cancelled_customer_low_churn_risk' for d in dq_warnings
            )

            if not cancellation_dqw_exists:
                dq_warnings.append({
                    'type': 'cancelled_customer_low_churn_risk',
                    'severity': 'CRITICAL',
                    'description': f"Customer has threatened_cancellation=True (30-day notice processed) but churn_probability is '{churn_probability}' (score: {churn_score}/100). This customer has ALREADY CHURNED - churn probability should be 'Very High'.",
                    'impact': "Agent will severely underestimate churn risk. Customer may already be lost but system suggests they're safe. ACTIVELY MISLEADS AGENTS.",
                    'recommendation': "Treat this customer as ALREADY CHURNED. Do not attempt retention outreach - focus on winback campaigns instead. Update threat_indicators.escalation_threats or override churn_probability to 'Very High'.",
                    'actual_state': {
                        'threatened_cancellation': True,
                        'churn_probability': churn_probability,
                        'churn_score': churn_score,
                        'recommended_probability': 'Very High'
                    }
                })
                summary_json['data_quality_warnings'] = dq_warnings
                logger.warning(f"  BUG FIX #7: CRITICAL DQW - Customer threatened cancellation but churn_risk={churn_probability} (score={churn_score}). Customer has ALREADY CHURNED!")
        elif churn_probability not in ['very high', 'high']:
            # Churn probability doesn't match the threatened cancellation status
            logger.info(f"  Note: threatened_cancellation=True but churn_probability={churn_probability} (score={churn_score}). May warrant review if churn risk is accurate.")

    # BUG FIX #13: Detect plan_count=0 but plan_revenue > 0 (stale revenue cache)
    # If customer has no active plans but revenue_breakdown shows plan revenue, this indicates data inconsistency
    # plan_count comes from Revenue_Cache (Oracle data), plan_revenue should match
    customer_profile = summary_json.get('customer_profile', {})
    plan_count = customer_profile.get('plan_count', 0) or 0  # BUG FIX: Use plan_count (sim_count removed)
    plan_revenue = customer_profile.get('monthly_revenue_plan') or 0  # Handle None values

    if plan_count == 0 and plan_revenue > 0:
        dq_warnings = summary_json.get('data_quality_warnings', [])

        # Check if DQW already exists
        revenue_sim_dqw_exists = any(
            d.get('type') == 'plan_revenue_no_sim_mismatch' for d in dq_warnings
        )

        if not revenue_sim_dqw_exists:
            dq_warnings.append({
                'type': 'plan_revenue_no_sim_mismatch',
                'severity': 'MEDIUM',
                'description': f"plan_count=0 but revenue_breakdown shows €{plan_revenue:.2f}/month from plans. Data inconsistency detected - either plan count is stale or plan revenue is inaccurate.",
                'impact': "Agents may see contradictory information. Revenue cache may be out of sync with Oracle CRM data.",
                'recommendation': "Run revenue cache refresh for this customer. Investigate if customer has active plans that aren't being counted, or if plan revenue is from cancelled/terminated services.",
                'actual_state': {
                    'plan_count': plan_count,  # BUG FIX: Use plan_count key
                    'plan_revenue': plan_revenue,
                    'revenue_source': 'Revenue_Cache'
                }
            })
            summary_json['data_quality_warnings'] = dq_warnings
            logger.warning(f"  BUG FIX #13: DQW added - plan_count=0 but plan_revenue=€{plan_revenue:.2f}/month (stale revenue cache?)")


    # CRITICAL FIX: Populate opportunity_actions from sentiment_analysis.evidence when LLM omits them
    # Python extracts revenue opportunities (family_plan, device_upgrade, etc.) into evidence,
    # but LLM may not surface them in opportunity_actions. Ensure they are visible to agents.
    evidence = summary_json.get('sentiment_analysis', {}).get('evidence', [])

    # Check if LLM populated opportunity_actions
    recommended = summary_json.get('recommended_actions', {})
    existing_opportunities = recommended.get('opportunity_actions', [])

    # Look for opportunity evidence that the LLM might have missed
    opportunity_types = ['family_plan_opportunity', 'device_upgrade_opportunity', 'postpaid_conversion_opportunity']
    missed_opportunities = [e for e in evidence if e.get('type') in opportunity_types]

    if missed_opportunities and not existing_opportunities:
        logger.info(f"  Found {len(missed_opportunities)} opportunity evidence items not in opportunity_actions - adding them")

        if 'opportunity_actions' not in recommended:
            recommended['opportunity_actions'] = []

        for opp in missed_opportunities:
            opp_type = opp.get('type')

            if opp_type == 'family_plan_opportunity':
                plan_count = opp.get('plan_count', 0)  # BUG FIX: Use plan_count key
                monthly_revenue = opp.get('monthly_revenue', 0)
                recommended['opportunity_actions'].append({
                    'action': f"Offer family plan savings consultation. Customer has {plan_count} plans with €{monthly_revenue:.0f}/month revenue. Family plan could provide better value and bundle discounts.",
                    'evidence': {
                        'current_state': f"{plan_count} plans/SIMs, €{monthly_revenue:.0f}/month",
                        'opportunity_details': f"Multiple plans on account ({plan_count}) indicates potential family/group usage. Family plan bundles typically offer better per-plan pricing.",
                        'value_proposition': f"Potential savings of 10-20% through family plan bundles with shared data and inclusive features.",
                        'reasoning': f"Revenue_Cache data shows {plan_count} plans at €{monthly_revenue:.0f}/month. {opp.get('reasoning')}",
                        'data_source': 'Revenue_Cache.plan_count'  # BUG FIX: Updated from sim_count
                    },
                    'priority': 'MEDIUM'
                })

            elif opp_type == 'device_upgrade_opportunity':
                # Check if this is from Customer_Device_Assets or Revenue_Cache
                if opp.get('source') == 'Revenue_Cache (calculated)':
                    # Revenue_Cache evidence - use handset_remaining_installments
                    remaining = opp.get('handset_remaining_installments')
                    monthly_device = opp.get('handset_monthly_installment', 0)

                    # Null-safe template - avoid "None" in output
                    if remaining:
                        if remaining == 1:
                            time_text = "1 payment remaining"
                        else:
                            time_text = f"{remaining} payments remaining"
                    else:
                        time_text = "ending soon"

                    amount_text = f"€{monthly_device:.2f}/month" if monthly_device else "current installment"

                    recommended['opportunity_actions'].append({
                        'action': f"Contact customer about device upgrade. Customer has {time_text} ({amount_text}). Optimal timing for upgrade discussion with new device options and potential trade-in value.",
                        'evidence': {
                            'current_state': f"{remaining} device payments remaining at €{monthly_device:.2f}/month" if remaining else "Device contract ending soon",
                            'opportunity_details': "Device payments ending soon - optimal timing to discuss renewal with new device selection.",
                            'value_proposition': "Latest devices with trade-in value can reduce monthly installment while upgrading features.",
                            'reasoning': f"{opp.get('reasoning')}",
                            'data_source': opp.get('data_source', 'Revenue_Cache.handset_remaining_installments')
                        },
                        'priority': 'MEDIUM'
                    })
                else:
                    # Customer_Device_Assets evidence
                    days_remaining = opp.get('days_remaining')
                    device_brand = opp.get('device_brand', 'device')
                    device_model = opp.get('device_model', 'contract')
                    monthly_installment = opp.get('monthly_installment', 0)

                    # Null-safe template
                    if days_remaining is not None:
                        days_text = f"{days_remaining} days"
                    else:
                        days_text = "soon"

                    recommended['opportunity_actions'].append({
                        'action': f"Contact customer about device upgrade. {device_brand} {device_model} contract ends in {days_text} (€{monthly_installment:.2f}/month). New device options available with trade-in.",
                        'evidence': {
                            'current_state': f"{device_brand} {device_model}, {days_remaining} days remaining" if days_remaining else f"{device_brand} {device_model}, contract ending soon",
                            'opportunity_details': f"Contract ending soon - optimal timing to discuss renewal with new device selection and potential plan upgrade.",
                            'value_proposition': f"Latest devices with trade-in value can reduce monthly installment while upgrading features.",
                            'reasoning': f"{opp.get('reasoning')}",
                            'data_source': 'Customer_Device_Assets.contract_end_date'
                        },
                        'priority': 'MEDIUM'
                    })

        logger.info(f"  Added {len(missed_opportunities)} opportunity actions from evidence array")

    # CRITICAL FIX: Populate threat_indicators from sentiment_analysis.evidence when LLM omits them
    # Python extracts threat evidence (cancellation_threat, escalation_threat, competitor_threat, etc.)
    # but LLM may not populate them in threat_indicators. Ensure they are visible to agents.
    evidence = summary_json.get('sentiment_analysis', {}).get('evidence', [])

    # Look for threat evidence that the LLM might have missed
    threat_types = ['cancellation_threat', 'escalation_threat', 'competitor_threat', 'regulatory_threat', 'legal_threat']
    missed_threats = [e for e in evidence if e.get('type') in threat_types]

    # Initialize threat_indicators if needed (may already exist from LLM or previous processing)
    if 'threat_indicators' not in summary_json:
        summary_json['threat_indicators'] = {}
    threat_ind = summary_json['threat_indicators']

    if missed_threats:
        # Initialize threat_indicators if missing
        if 'threat_indicators' not in summary_json:
            summary_json['threat_indicators'] = {}

        threat_ind = summary_json['threat_indicators']

        # Process each threat type
        for threat in missed_threats:
            threat_type = threat.get('type')

            if threat_type == 'cancellation_threat':
                # Ensure cancellation_threats section exists
                if 'cancellation_threats' not in threat_ind:
                    threat_ind['cancellation_threats'] = {
                        'threatened_cancellation': False,
                        'cancellation_reason': None,
                        'evidence': []
                    }

                canc = threat_ind['cancellation_threats']
                canc['threatened_cancellation'] = True
                if not canc.get('cancellation_reason'):
                    # Infer reason from quote
                    quote = threat.get('quote', '').lower()
                    if 'price' in quote or 'cost' in quote or 'expensive' in quote:
                        canc['cancellation_reason'] = 'pricing'
                    elif 'service' in quote or 'quality' in quote:
                        canc['cancellation_reason'] = 'service_quality'
                    elif 'unresolved' in quote or 'issue' in quote or 'problem' in quote:
                        canc['cancellation_reason'] = 'unresolved_issue'
                    elif 'three' in quote or 'vodafone' in quote or 'competitor' in quote:
                        canc['cancellation_reason'] = 'competitor_offer'
                    else:
                        canc['cancellation_reason'] = 'other'

                canc['evidence'].append({
                    'quote': threat.get('quote', ''),
                    'interaction_id': threat.get('interaction_id'),
                    'date': threat.get('date')
                })

                # CRITICAL FIX: Cancellation is NOT an escalation threat
                # Cancellation is a churn risk, not an escalation to a higher authority
                # Removing the logic that incorrectly marked cancellation as an escalation

            elif threat_type == 'escalation_threat':
                if 'escalation_threats' not in threat_ind:
                    threat_ind['escalation_threats'] = {
                        'threatened_escalation': False,
                        'escalation_target': None,
                        'evidence': []
                    }

                esc = threat_ind['escalation_threats']
                esc['threatened_escalation'] = True
                if not esc.get('escalation_target'):
                    esc['escalation_target'] = threat.get('escalation_target', 'manager')
                esc['evidence'].append({
                    'quote': threat.get('quote', ''),
                    'interaction_id': threat.get('interaction_id'),
                    'date': threat.get('date')
                })

        logger.info(f"  Populated threat_indicators from {len(missed_threats)} threat evidence items")

        # BUG FIX #14: Standardize evidence array types (ensure all items are objects, not strings)
        # LLM may generate mixed-type evidence arrays with both plain strings and objects
        # This causes TypeError in consumers that expect uniform object types
        for threat_type in ['cancellation_threats', 'escalation_threats', 'regulatory_threats', 'legal_threats']:
            if threat_type in threat_ind:
                evidence = threat_ind[threat_type].get('evidence', [])
                if evidence:
                    # Check if any items are plain strings (not objects)
                    has_strings = any(isinstance(item, str) for item in evidence)
                    if has_strings:
                        logger.warning(f"  BUG FIX #14: Found plain strings in {threat_type}.evidence - converting to objects")
                        # Convert strings to objects with a 'quote' field
                        standardized_evidence = []
                        for item in evidence:
                            if isinstance(item, str):
                                # Convert string to object
                                standardized_evidence.append({'quote': item})
                            elif isinstance(item, dict):
                                # Already an object - keep as-is
                                standardized_evidence.append(item)
                            else:
                                # Unknown type - try to convert to string
                                standardized_evidence.append({'quote': str(item)})
                        threat_ind[threat_type]['evidence'] = standardized_evidence
                        logger.info(f"  BUG FIX #14: Standardized {threat_type}.evidence to all objects (converted {len([i for i in evidence if isinstance(i, str)])} strings)")


        # CRITICAL FIX: Remove duplicate evidence between cancellation_threats and escalation_threats
        # A cancellation threat is NOT an escalation threat unless it specifically demands escalation
        # Remove cancellation-only quotes from escalation_threats.evidence
        if 'cancellation_threats' in threat_ind and 'escalation_threats' in threat_ind:
            canc_evidence = threat_ind['cancellation_threats'].get('evidence', [])
            esc_evidence = threat_ind['escalation_threats'].get('evidence', [])

            if canc_evidence and esc_evidence:
                # Check for duplicates (same quote, date, interaction_id)
                canc_signatures = set()
                for item in canc_evidence:
                    sig = (item.get('quote', '')[:50], item.get('date'), item.get('interaction_id'))
                    canc_signatures.add(sig)

                # Filter escalation evidence to remove items that are only about cancellation
                filtered_esc_evidence = []
                removed_count = 0

                for item in esc_evidence:
                    sig = (item.get('quote', '')[:50], item.get('date'), item.get('interaction_id'))
                    if sig in canc_signatures:
                        # Check if this is truly an escalation (contains escalation keywords)
                        quote = item.get('quote', '') or ''
                        quote_lower = quote.lower()
                        escalation_keywords = ['manager', 'supervisor', 'ceo', 'complaint', 'escalate', 'speak to', 'higher up', 'your boss']

                        if not any(kw in quote_lower for kw in escalation_keywords):
                            # This is just a cancellation, not an escalation - remove it
                            removed_count += 1
                            continue

                    filtered_esc_evidence.append(item)

                if removed_count > 0:
                    threat_ind['escalation_threats']['evidence'] = filtered_esc_evidence
                    logger.info(f"  Removed {removed_count} duplicate cancellation quote(s) from escalation_threats.evidence (not true escalations)")

    # CRITICAL FIX: Ensure customer_profile.revenue_breakdown text matches structured fields
    # A cancellation threat is NOT an escalation threat unless it specifically demands escalation
    # Remove cancellation-only quotes from escalation_threats.evidence
    if 'cancellation_threats' in threat_ind and 'escalation_threats' in threat_ind:
        canc_evidence = threat_ind['cancellation_threats'].get('evidence', [])
        esc_evidence = threat_ind['escalation_threats'].get('evidence', [])

        if canc_evidence and esc_evidence:
            # Check for duplicates (same quote, date, interaction_id)
            canc_signatures = set()
            for item in canc_evidence:
                sig = (item.get('quote', '')[:50], item.get('date'), item.get('interaction_id'))
                canc_signatures.add(sig)

            # Filter escalation evidence to remove items that are only about cancellation
            filtered_esc_evidence = []
            removed_count = 0

            for item in esc_evidence:
                sig = (item.get('quote', '')[:50], item.get('date'), item.get('interaction_id'))
                if sig in canc_signatures:
                    # Check if this is truly an escalation (contains escalation keywords)
                    quote = item.get('quote', '').lower()
                    escalation_keywords = ['manager', 'supervisor', 'ceo', 'complaint', 'escalate', 'speak to', 'higher up', 'your boss']

                    if not any(kw in quote for kw in escalation_keywords):
                        # This is just a cancellation, not an escalation - remove it
                        removed_count += 1
                        continue

                filtered_esc_evidence.append(item)

            if removed_count > 0:
                threat_ind['escalation_threats']['evidence'] = filtered_esc_evidence
                logger.info(f"  Removed {removed_count} duplicate cancellation quote(s) from escalation_threats.evidence (not true escalations)")

    # BUG FIX #17: Validate switching_intent vs porting directionality
    # If evidence shows customer is porting TO Virgin Media (acquisition), switching_intent should be False
    competitor_threats = threat_ind.get('competitor_threats', {})
    if competitor_threats:
        switching_intent = competitor_threats.get('switching_intent')
        evidence = competitor_threats.get('evidence', [])

        # Only validate if switching_intent is currently True (potential churn threat)
        if switching_intent is True and evidence:
            # Check evidence for porting acquisition patterns
            acquisition_patterns = [
                'porting from', 'joining from', 'moving from', 'switching from',
                'to virgin', 'to virgin media', 'joining virgin', 'coming to virgin'
            ]
            threat_patterns = [
                'leaving virgin', 'switching to', 'moving to', 'going to',
                'virgin to', 'leaving for'
            ]

            # Combine all evidence text for analysis
            all_evidence_text = ' '.join([
                (item.get('quote', '') if isinstance(item, dict) else str(item))
                for item in evidence
            ]).lower()

            # Check for acquisition patterns (customer joining Virgin)
            has_acquisition_pattern = any(pattern in all_evidence_text for pattern in acquisition_patterns)

            # Check for threat patterns (customer leaving Virgin)
            has_threat_pattern = any(pattern in all_evidence_text for pattern in threat_patterns)

            # If evidence strongly indicates acquisition (joining FROM competitor), fix switching_intent
            if has_acquisition_pattern and not has_threat_pattern:
                logger.warning(f"  BUG FIX #17: Incorrect switching_intent=True detected! Evidence shows customer is porting TO Virgin Media (acquisition, not churn threat). Fixing to switching_intent=False.")
                competitor_threats['switching_intent'] = False

                # Add data quality warning
                dq_warnings = summary_json.get('data_quality_warnings', [])
                dq_warnings.append({
                    'type': 'SWITCHING_INTENT_MISCLASSIFIED',
                    'severity': 'MEDIUM',
                    'description': 'switching_intent was incorrectly set to True when evidence shows customer is joining Virgin Media from a competitor (acquisition, not churn threat). Fixed to False.',
                    'impact': 'Customer would be incorrectly classified as churn risk when actually a growth/acquisition opportunity.',
                    'evidence_sample': evidence[0] if evidence else None
                })
                summary_json['data_quality_warnings'] = dq_warnings

    # CRITICAL FIX: Ensure customer_profile.revenue_breakdown text matches structured fields
    # LLM may generate incorrect breakdown (e.g., "€0 from devices" when monthly_revenue_device = 35)
    customer_profile = summary_json.get('customer_profile', {})
    monthly_total = customer_profile.get('monthly_revenue_total', 0) or 0
    monthly_plan = customer_profile.get('monthly_revenue_plan', 0) or 0
    monthly_device = customer_profile.get('monthly_revenue_device', 0) or customer_profile.get('device_financing_revenue', 0) or 0
    monthly_fixed = customer_profile.get('monthly_revenue_fixed', 0) or 0

    # CRITICAL FIX: Use monthly_revenue_total as source of truth, but check for discrepancies
    # If total != plan + device + fixed, there may be discounts or data sync issues
    calculated_sum = monthly_plan + monthly_device + monthly_fixed

    # Generate breakdown - prioritize using actual component breakdown if available
    breakdown_parts = []
    if monthly_plan > 0:
        breakdown_parts.append(f"€{monthly_plan:.0f} from plans")
    if monthly_device > 0:
        breakdown_parts.append(f"€{monthly_device:.0f} from devices")
    if monthly_fixed > 0:
        breakdown_parts.append(f"€{monthly_fixed:.0f} from fixed")

    # Use monthly_revenue_total for the total, and note discrepancy if exists
    if breakdown_parts:
        breakdown_text = ", ".join(breakdown_parts)
        # Check if the sum matches the total
        if abs(calculated_sum - monthly_total) > 0.01:  # More than 1 cent difference
            # There's a discrepancy - could be discount or data issue
            discount_amount = calculated_sum - monthly_total
            breakdown_text += f" (€{monthly_total:.0f} total after €{discount_amount:.0f} discount)"
            logger.info(f"  Revenue discrepancy detected: plan/device/fixed sum €{calculated_sum:.0f} but total €{monthly_total:.0f}. Breakdown includes discount note.")
    else:
        breakdown_text = f"€{monthly_total:.0f}/month" if monthly_total > 0 else "No revenue data"

    # Update the customer_profile.revenue_breakdown if it's wrong or missing
    existing_breakdown = customer_profile.get('revenue_breakdown', '')
    if not existing_breakdown or ('€0 from devices' in existing_breakdown and monthly_device > 0):
        summary_json['customer_profile']['revenue_breakdown'] = breakdown_text
        logger.info(f"  Corrected revenue_breakdown: {breakdown_text}")

    # CRITICAL FIX: Detect fully churned customers and update cancellation-related issues
    # When mobile_active=False AND fixed_active=False AND sim_count=0, customer has fully churned
    # Update cancellation issues from "In Progress" to "Completed - Service Terminated"
    # Clear opportunity actions - agent should not follow up with churned customers
    mobile_active = customer_profile.get('mobile_active', False)
    fixed_active = customer_profile.get('fixed_active', False)
    sim_count = customer_profile.get('plan_count', 0) or 0  # Use plan_count

    # B7-C3 FIX: Derive service_status from open Pega/ServiceNow cases, not just static flag
    # Check for service-impacting cases that should override the account status flag
    key_issues = summary_json.get('key_issues', [])
    issue_resolution_actions = summary_json.get('issue_resolution_actions', [])
    all_issues = key_issues + issue_resolution_actions

    service_impacting_keywords = [
        'unable to make', 'cannot make', 'cannot receive', 'no service',
        'call failure', 'call barring', 'outbound blocked', 'inbound blocked',
        'service degradation', 'service outage', 'no dial tone',
        'porting failure', 'sim not working', 'network issue'
    ]

    mobile_degraded = False
    fixed_degraded = False

    for issue in all_issues:
        issue_text = issue.get('issue', '').lower() + ' ' + issue.get('description', '').lower()
        status = issue.get('status', '').lower()

        # Only consider open/in-progress issues
        if 'progress' in status or 'open' in status or 'pending' in status:
            # Check for mobile service issues
            if any(kw in issue_text for kw in service_impacting_keywords):
                if 'fixed' not in issue_text or 'mobile' in issue_text or 'call' in issue_text or 'sim' in issue_text:
                    mobile_degraded = True
                    logger.info(f"  B7-C3 FIX: Detected mobile service degradation issue: {issue.get('issue', '')[:60]}")

    # Derive service_status based on active flags and open issues
    service_status_parts = []
    if mobile_active:
        if mobile_degraded:
            service_status_parts.append('Mobile: Degraded')
        else:
            service_status_parts.append('Mobile: Active')
    else:
        service_status_parts.append('Mobile: Inactive')

    if fixed_active:
        service_status_parts.append('Fixed: Active')
    else:
        service_status_parts.append('Fixed: Inactive')

    service_status = ', '.join(service_status_parts)

    # Update customer_profile.service_status in summary
    if 'customer_profile' in summary_json:
        summary_json['customer_profile']['service_status'] = service_status

    # Check for FULL CHURN (all services inactive)
    is_fully_churned = (
        not mobile_active and
        not fixed_active and
        (sim_count == 0 or sim_count is None) and
        service_status and 'inactive' in service_status.lower()
    )

    if is_fully_churned:
        logger.info(f"  Customer {customer_id} detected as FULLY CHURNED - all services inactive")

        # Find and update cancellation-related issues
        key_issues = summary_json.get('key_issues', [])
        cancellation_issues_updated = False

        for issue in key_issues:
            issue_text = issue.get('issue', '').lower()
            status = issue.get('status', '')

            # Update cancellation-related issues that are still "In Progress"
            if 'cancel' in issue_text and 'progress' in status.lower():
                issue['status'] = 'Completed - Service Terminated'
                issue['resolution_notes'] = 'ServiceSight Intelligence detected customer has fully churned (all services inactive). Cancellation completed.'
                cancellation_issues_updated = True
                logger.info(f"  Updated cancellation issue status to 'Completed - Service Terminated'")

        if cancellation_issues_updated:
            # Clear opportunity actions - no upsell to churned customers
            recommended = summary_json.get('recommended_actions', {})
            if recommended.get('opportunity_actions'):
                recommended['opportunity_actions'] = []
                logger.info(f"  Cleared opportunity_actions for churned customer")

    # B-037 FIX: Validate case_id fields in key_issues and blocking_issues
    # case_id must only contain actual system case reference numbers (INC####, CC-####, SN-####)
    # Free-text descriptions should not be in the case_id field
    key_issues = summary_json.get('key_issues', [])
    case_id_validation_issues = []

    # Valid case ID patterns: INC1234567, CC-12345, SN-12345, PZ-ABC123, etc.
    valid_case_id_pattern = re.compile(r'^(INC[0-9]+|CC-[0-9]+|SN-[0-9]+|PZ-[A-Z0-9]+|[A-Z]{2}-[0-9]+)$', re.IGNORECASE)

    # Check key_issues
    for issue in key_issues:
        case_id = issue.get('case_id', '')
        if case_id and case_id != 'Unknown':
            # Check if case_id matches a valid pattern
            if not valid_case_id_pattern.match(case_id):
                # case_id contains free-text - this is invalid
                case_id_validation_issues.append({
                    'location': 'key_issues',
                    'issue': issue.get('issue', 'Unknown')[:60],
                    'invalid_case_id': case_id,
                    'corrective_action': 'Remove case_id field or use actual case reference number'
                })
                # Remove the invalid case_id
                del issue['case_id']
                logger.warning(f"  B-037 FIX: Removed invalid case_id '{case_id}' from key_issue (free-text, not a valid case reference)")

    # BUG FIX #14: Also check blocking_issues case_ids
    blocking_issues = summary_json.get('blocking_issues', [])
    for issue in blocking_issues:
        case_id = issue.get('case_id', '')
        if case_id and case_id != 'Unknown':
            # Check if case_id matches a valid pattern
            if not valid_case_id_pattern.match(case_id):
                # case_id contains free-text - this is invalid
                case_id_validation_issues.append({
                    'location': 'blocking_issues',
                    'issue': issue.get('title', issue.get('issue', 'Unknown'))[:60],
                    'invalid_case_id': case_id,
                    'corrective_action': 'Remove case_id field or use actual case reference number'
                })
                # Remove the invalid case_id
                del issue['case_id']
                logger.warning(f"  BUG FIX #14: Removed invalid case_id '{case_id}' from blocking_issue (free-text, not a valid case reference)")

    if case_id_validation_issues:
        dq_warnings = summary_json.get('data_quality_warnings', [])
        dq_warnings.append({
            'type': 'CASE_ID_FORMAT_INVALID',
            'severity': 'MEDIUM',
            'description': f'{len(case_id_validation_issues)} issue(s) had invalid case_id format (free-text instead of system reference). Invalid case_id fields have been removed.',
            'impact': 'Case identification was corrupted with free-text descriptions. This affects case traceability and lookup.',
            'recommendation': 'Ensure case_id field only contains valid system case references (INC####, CC-####, SN-####). Free-text issue descriptions should go in the "issue" field, not case_id.',
            'examples': case_id_validation_issues[:3]
        })
        summary_json['data_quality_warnings'] = dq_warnings
        logger.info(f"  B-037 FIX: Validated case_id fields - {len(case_id_validation_issues)} invalid case_id(s) removed")

    # B-036 FIX: Resolution signal detection in call transcripts
    # Check if transcript contains resolution patterns but issue is still marked Open
    resolution_patterns = [
        'payment processed', 'payment successful', 'payment completed',
        'issue resolved', 'sorted now', 'fixed', 'working now', 'resolved',
        'problem solved', 'all good', 'no longer an issue', 'thank you'
    ]

    call_recordings = payload.get('call_recordings', [])
    transcript_has_resolution = False

    for recording in call_recordings:
        call_summary = recording.get('call_summary', '') or recording.get('summary', '')
        if call_summary and isinstance(call_summary, str):
            call_summary_lower = call_summary.lower()
            for pattern in resolution_patterns:
                if pattern in call_summary_lower:
                    transcript_has_resolution = True
                    break
        if transcript_has_resolution:
            break

    # Check for Open issues that may be resolved
    if transcript_has_resolution:
        key_issues = summary_json.get('key_issues', [])
        potential_resolutions = []

        for issue in key_issues:
            status = issue.get('status', '').upper()
            if 'OPEN' in status or 'IN PROGRESS' in status:
                potential_resolutions.append({
                    'issue': issue.get('issue', 'Unknown')[:60],
                    'case_id': issue.get('case_id', 'Unknown'),
                    'current_status': status
                })

        if potential_resolutions:
            dq_warnings = summary_json.get('data_quality_warnings', [])
            dq_warnings.append({
                'type': 'RESOLUTION_SIGNAL_DETECTED',
                'severity': 'MEDIUM',
                'description': f'Call transcript contains resolution confirmation patterns (e.g., "payment processed", "issue resolved") but {len(potential_resolutions)} issue(s) are still marked Open/In Progress. Transcript indicates resolution may have occurred.',
                'impact': 'Issue status may be stale. Customer may have been told the issue is resolved during the call, but the case hasn\'t been updated in the system yet.',
                'recommendation': 'Verify case status in Pega/ServiceNow. If confirmed resolved, update case status. Consider adding transcript-based status detection pipeline.',
                'potentially_resolved_issues': potential_resolutions[:5]
            })
            summary_json['data_quality_warnings'] = dq_warnings
            logger.info(f"  B-036 FIX: Resolution signals detected in transcript - {len(potential_resolutions)} Open issue(s) may be resolved")

            # FIX: Use summary_json.get() to avoid NameError when recommended is not in scope
            # Also, this is about resolution, NOT churn - use VERIFY_RESOLUTION
            recommended = summary_json.get('recommended_actions', {})
            if recommended:
                recommended['priority_focus'] = 'VERIFY_RESOLUTION'
                logger.info(f"  B-036 FIX: Set priority_focus to VERIFY_RESOLUTION (resolution signals detected, not churn)")

                # Add verification note (NOT churn note) to interaction summary
                if 'interaction_summary' in summary_json:
                    existing_summary = summary_json['interaction_summary'].get('summary', '')
                    resolution_note = " [RESOLUTION SIGNAL DETECTED: Transcript contains phrases like 'payment processed' or 'issue resolved'. Verify case status in Pega/ServiceNow.]"
                    summary_json['interaction_summary']['summary'] = existing_summary + resolution_note
                    logger.info(f"  B-036 FIX: Added resolution verification note to interaction_summary")

    # Add input data summary - shows what data was used to generate this summary
    input_summary = build_input_data_summary(payload)
    summary_json['input_data_summary'] = input_summary

    # Add execution timing data - distinguishes between data date vs execution date
    execution_date = datetime.now()
    summary_json['customer_id'] = customer_id
    summary_json['summary_generated_at'] = execution_date.isoformat()
    summary_json['summary_for_date'] = str(run_date)
    summary_json['watermark'] = execution_date.isoformat()

    # NEW: Execution timing information
    # Helps distinguish "date of customer data" from "date summary was generated"
    summary_json['execution_metadata'] = {
        'execution_date': execution_date.strftime('%Y-%m-%d'),
        'execution_time': execution_date.strftime('%H:%M:%S'),
        'execution_timestamp': execution_date.isoformat(),
        'data_as_of_date': input_summary.get('time_period', {}).get('latest_interaction'),
        'data_freshness_days': calculate_data_freshness(input_summary, execution_date),
        'summary_version': 'LLM_v5',
        'model_used': LLM_CONFIG.get('deployment_name', 'gpt-4o'),
        'generation_scenario': scenario  # FULL, INCREMENTAL, or REBUILD
    }

    # Add prominent date display at the top level for visibility
    # This addresses the user request: "I need to show a date near the AI summary"
    summary_json['summary_date_display'] = f"Generated on {execution_date.strftime('%Y-%m-%d at %H:%M')}"

    # B7-C4 FIX: Add summary_generated_at timestamp for deduplication
    # This helps prevent duplicate processing of the same customer in batch runs
    summary_json['summary_generated_at'] = execution_date.isoformat()

    # CRITICAL FIX: Check for stale days_open values in open issues
    # When a summary is viewed days/weeks after generation, days_open values for open issues become stale
    # For example, an issue that was "1 day old" on Jan 30 is "28 days old" on Feb 27
    data_as_of = input_summary.get('time_period', {}).get('latest_interaction')
    if data_as_of:
        try:
            data_date = datetime.fromisoformat(data_as_of.replace('Z', '+00:00'))
            days_since_data = (execution_date - data_date).days

            # Check for open issues that would have stale days_open values
            key_issues = summary_json.get('key_issues', [])
            open_issues = [i for i in key_issues if i.get('status', '').lower() not in ['resolved', 'closed', 'completed', 'cancelled']]

            if open_issues and days_since_data > 7:
                # Summary is more than 7 days old and has open issues
                dq_warnings = summary_json.get('data_quality_warnings', [])
                stale_warning_exists = any('stale days_open' in w.get('description', '').lower() for w in dq_warnings)

                if not stale_warning_exists:
                    dq_warnings.append({
                        'type': 'stale_issue_age',
                        'severity': 'HIGH' if days_since_data > 14 else 'MEDIUM',
                        'description': f'This summary was generated {days_since_data} days ago (data as of {data_as_of[:10]}). Open issue ages (days_open_or_resolved) are calculated from generation date, not today. An issue listed as "1 day old" is actually {1 + days_since_data} days old today.',
                        'recommendation': f'Regenerate summary to get accurate issue ages. Current values are {days_since_data} days stale. For urgent issues, verify actual age in Pega/ServiceNow.',
                        'impact': f'{len(open_issues)} open issue(s) with stale age data. May understate urgency.',
                        'days_stale': days_since_data,
                        'data_as_of_date': data_as_of[:10],
                        'current_date': execution_date.strftime('%Y-%m-%d')
                    })
                    summary_json['data_quality_warnings'] = dq_warnings
                    logger.warning(f"  Added DQW: Summary is {days_since_data} days old with {len(open_issues)} open issues - days_open values are stale")
        except Exception as e:
            logger.debug(f"  Could not check for stale days_open: {e}")

    # DATA QUALITY CHECK: Detect revenue/service status inconsistencies
    # Check for customers marked as active but with 0 revenue or 0 plans
    customer_profile = summary_json.get('customer_profile', {})
    mobile_active = customer_profile.get('mobile_active', False)
    plan_count = customer_profile.get('plan_count', 0)  # BUG FIX: Use plan_count (sim_count removed from DB)
    monthly_revenue_total = customer_profile.get('monthly_revenue_total')
    monthly_revenue_plan = customer_profile.get('monthly_revenue_plan')

    inconsistencies = []
    if mobile_active and plan_count == 0:
        inconsistencies.append(f"mobile_active=True but plan_count=0 (no active plans)")

    if mobile_active and plan_count > 0:
        # Has active service with plan(s), check if revenue is missing
        if monthly_revenue_plan is None or monthly_revenue_plan == 0:
            inconsistencies.append(f"mobile_active=True with {plan_count} plan(s) but monthly_revenue_plan={monthly_revenue_plan}")

    if mobile_active and (monthly_revenue_total is None or monthly_revenue_total == 0):
        inconsistencies.append(f"mobile_active=True but monthly_revenue_total={monthly_revenue_total}")

    if inconsistencies:
        dq_warnings = summary_json.get('data_quality_warnings', [])
        revenue_dqw_exists = any('revenue_status_mismatch' in w.get('type', '') for w in dq_warnings)

        if not revenue_dqw_exists:
            dq_warnings.append({
                'type': 'revenue_status_mismatch',
                'severity': 'HIGH',
                'description': f"Customer service status and revenue data are inconsistent: {', '.join(inconsistencies)}.",
                'impact': "Customer appears active but revenue/SIM data suggests inactive or free service. May indicate: 1) Customer in disconnection process, 2) Free plan (revenue=€0), 3) Data sync issue between Oracle and cache, 4) Revenue data not captured correctly.",
                'recommendation': "Verify customer status in Oracle CRM. Check if customer is legitimately active with €0 revenue (free plan) or if there's a data synchronization issue. Consider manual review.",
                'inconsistencies': inconsistencies,
                'data_snapshot': {
                    'mobile_active': mobile_active,
                    'plan_count': plan_count,  # BUG FIX: Use plan_count (sim_count removed from DB)
                    'monthly_revenue_total': monthly_revenue_total,
                    'monthly_revenue_plan': monthly_revenue_plan
                }
            })
            summary_json['data_quality_warnings'] = dq_warnings
            logger.warning(f"  Added DQW: Revenue/status mismatch - {len(inconsistencies)} inconsistency(ies) detected")

    # ============================================================
    # FINAL VALIDATION AND RECONCILIATION LAYER
    # ============================================================
    # CRITICAL: This is the FINAL consistency check before saving to database.
    # Runs AFTER LLM generation and Python post-processing.
    # Enforces single source of truth, cross-field consistency, and business rules.
    # Prevents entire classes of bugs: frustration_level/score mismatches,
    # health_score reasoning with wrong numbers, churn_indicator contradictions, etc.
    try:
        from summary_validator import validate_and_reconcile

        logger.info("  Running final validation and reconciliation...")
        summary_json, validation_report = validate_and_reconcile(summary_json)

        if validation_report.issues_found > 0:
            logger.info(f"  Validation complete: {validation_report.issues_found} issues found, {validation_report.issues_fixed} fixed")

        if not validation_report.is_valid:
            logger.warning(f"  [{customer_id}] Validation report: {validation_report.summary()}")
            # Log unfixed issues for monitoring
            unfixed = validation_report.unfixed_issues
            for issue in unfixed[:5]:  # First 5 unfixed issues
                logger.warning(f"    [{issue.severity}] {issue.code}: {issue.description}")

        # Validation metadata is already added to summary_json by validate_and_reconcile()
    except ImportError:
        logger.warning("  summary_validator not available - skipping validation layer")
    except Exception as e:
        logger.error(f"  Validation layer error: {e}")

    return {
        'summary': summary_json,
        'tokens': llm_result,
        'is_full_build': is_full_build,  # For database: update last_full_build_date
        'scenario': scenario  # For logging/debugging
    }


# ============================================================
# MAIN PROCESSING LOGIC
# ============================================================

def process_customers(run_date, worker_id=None, batch_size=BATCH_SIZE):
    """Main processing loop."""

    logger.info("=" * 60)
    logger.info("LLM SUMMARISER v5 - Explainable AI Edition")
    logger.info("=" * 60)
    logger.info(f"Run Date: {run_date}")
    logger.info(f"Worker: {worker_id or 'default'}")
    logger.info(f"Batch Size: {batch_size}")
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    conn = get_connection()
    cursor = conn.cursor()

    # Claim batch
    if worker_id:
        cursor.execute("EXEC dbo.sp_ClaimSummaryBatch @WorkerID = ?, @BatchSize = ?", worker_id, batch_size)
    else:
        cursor.execute("EXEC dbo.sp_ClaimSummaryBatch @WorkerID = 'DEFAULT', @BatchSize = ?", batch_size)

    conn.commit()

    # Fetch claimed customers
    cursor.execute("""
        SELECT customer_id, watermark, scenario
        FROM dbo.vw_CustomersPendingSummary
        WHERE status = 'IN_PROGRESS'
          AND processing_worker = ?
        ORDER BY claimed_at
    """, worker_id or 'DEFAULT')

    customers = cursor.fetchall()
    logger.info(f"Claimed {len(customers)} customers")

    if not customers:
        logger.info("No customers to process")
        conn.close()
        return

    # B7-C4 FIX: Batch pre-flight deduplication check
    # Check if any claimed customers have recent summaries (within current batch window)
    # to prevent duplicate processing of the same customer in concurrent runs
    batch_start_time = datetime.now()

    # Fetch existing summary timestamps for claimed customers
    claimed_customer_ids = [str(row[0]) for row in customers]
    placeholders = ','.join(['?' for _ in claimed_customer_ids])

    cursor.execute(f"""
        SELECT customer_id, updated_date, summary_generated_at
        FROM dbo.LLM_Customer_Summary
        WHERE customer_id IN ({placeholders})
    """, claimed_customer_ids)

    existing_summaries = {row[0]: (row[1], row[2]) for row in cursor.fetchall()}

    # Filter out customers with recent summaries (within last 30 minutes to avoid double-processing)
    customers_to_process = []
    skipped_customers = []

    for row in customers:
        customer_id = str(row[0])
        if customer_id in existing_summaries:
            updated_date, generated_at = existing_summaries[customer_id]

            # Check if summary was updated very recently (possible duplicate batch claim)
            if updated_date:
                time_diff = (batch_start_time - updated_date).total_seconds()
                if time_diff < 1800:  # 30 minutes
                    logger.warning(f"  B7-C4 FIX: Skipping customer {customer_id} - summary updated {time_diff/60:.1f} minutes ago (possible duplicate)")
                    skipped_customers.append(customer_id)
                    continue

            # Also check summary_generated_at if available
            if generated_at:
                try:
                    gen_time = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
                    time_diff = (batch_start_time - gen_time).total_seconds()
                    if time_diff < 1800:  # 30 minutes
                        logger.warning(f"  B7-C4 FIX: Skipping customer {customer_id} - summary generated {time_diff/60:.1f} minutes ago (possible duplicate)")
                        skipped_customers.append(customer_id)
                        continue
                except Exception:
                    pass

        customers_to_process.append(row)

    if skipped_customers:
        logger.info(f"  B7-C4 FIX: Skipped {len(skipped_customers)} recently processed customers to prevent duplicates: {', '.join(skipped_customers)}")

    if not customers_to_process:
        logger.info("No customers to process after deduplication check")
        conn.close()
        return

    customers = customers_to_process
    logger.info(f"After deduplication: {len(customers)} customers to process")

    # Process each customer
    processed = 0
    failed = 0
    total_tokens = 0
    total_cost = 0

    for row in customers:
        customer_id = row[0]
        watermark = row[1]

        try:
            result = process_customer(conn, customer_id, run_date, watermark)

            if result:
                # Save to database
                summary_json = json.dumps(result['summary'], ensure_ascii=False, default=str)

                # Check if update or insert
                cursor.execute("""
                    SELECT COUNT(*) FROM dbo.LLM_Customer_Summary WHERE customer_id = ?
                """, customer_id)

                exists = cursor.fetchone()[0] > 0

                is_full_build = result.get('is_full_build', 0)

                if exists:
                    cursor.execute("""
                        UPDATE dbo.LLM_Customer_Summary
                        SET summary_json = ?,
                            updated_date = GETDATE(),
                            last_full_build_date = CASE
                                WHEN ? = 1 THEN CAST(GETDATE() AS DATE)
                                ELSE last_full_build_date
                            END
                        WHERE customer_id = ?
                    """, summary_json, is_full_build, customer_id)

                    if is_full_build:
                        logger.info(f"  Last full build date updated to today")
                else:
                    cursor.execute("""
                        INSERT INTO dbo.LLM_Customer_Summary
                        (customer_id, summary_json, insert_date, updated_date, last_full_build_date)
                        VALUES (?, ?, GETDATE(), GETDATE(), CASE WHEN ? = 1 THEN CAST(GETDATE() AS DATE) ELSE NULL END)
                    """, customer_id, summary_json, is_full_build)

                    if is_full_build:
                        logger.info(f"  Last full build date set to today")

                # Mark as completed
                cursor.execute("EXEC dbo.sp_MarkSummaryCompleted @CustomerID = ?", customer_id)

                # Log tokens
                tokens = result['tokens']
                scenario = result.get('scenario', 'FULL')
                total_tokens += tokens['total_tokens']
                total_cost += tokens['total_cost']

                processed += 1
                logger.info(f"  [OK] Processed {customer_id} ({scenario}, tokens: {tokens['total_tokens']:,}, cost: ${tokens['total_cost']:.4f})")

            else:
                # Mark as failed
                cursor.execute("EXEC dbo.sp_MarkSummaryFailed @CustomerID = ?", customer_id)
                failed += 1
                logger.warning(f"  [FAIL] Failed {customer_id}")

            conn.commit()

        except Exception as e:
            logger.error(f"  [ERROR] Error processing {customer_id}: {e}")
            logger.debug(traceback.format_exc())

            # Mark as failed
            try:
                cursor.execute("EXEC dbo.sp_MarkSummaryFailed @CustomerID = ?", customer_id)
                conn.commit()
            except Exception:
                pass

            failed += 1

    cursor.close()
    conn.close()

    # Summary
    logger.info("=" * 60)
    logger.info("PROCESSING COMPLETE")
    logger.info("=" * 60)
    logger.info(f"Processed: {processed}")
    logger.info(f"Failed: {failed}")
    logger.info(f"Total Tokens: {total_tokens:,}")
    logger.info(f"Total Cost: ${total_cost:.4f}")
    logger.info(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)


# ============================================================
# ENTRY POINT
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Customer 360 LLM Summariser v5 - Explainable AI Edition",
        formatter_class=argparse.RawTextHelpFormatter
    )

    parser.add_argument("--run-date", type=str, help="Run date (YYYY-MM-DD)")
    parser.add_argument("--worker-id", type=str, help="Worker ID for concurrent processing")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Batch size")
    parser.add_argument("action", nargs="?", default="process",
                       choices=["process", "status", "retry", "help", "customer"],
                       help="Action to perform")
    parser.add_argument("customer_id", nargs="?", help="Customer ID (when action='customer')")

    args = parser.parse_args()

    # Determine run date
    if args.run_date:
        run_date = datetime.strptime(args.run_date, "%Y-%m-%d").date()
    else:
        run_date = date.today()

    if args.action == "status":
        # Show status
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT status, COUNT(*) as count
            FROM dbo.LLM_Customer_Summary
            GROUP BY status
            ORDER BY status
        """)
        print("\nLLM Customer Summary Status:")
        print("-" * 40)
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]:,}")
        cursor.close()
        conn.close()

    elif args.action == "retry":
        # Reset stuck summaries
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("EXEC dbo.sp_ResetStuckSummaries @TimeoutMinutes = ?", STUCK_TIMEOUT_MINUTES)
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Reset summaries stuck for > {STUCK_TIMEOUT_MINUTES} minutes")

    elif args.action == "help":
        print(__doc__)

    elif args.action == "customer":
        # Process single customer
        customer_id = args.customer_id
        if not customer_id:
            print("Usage: python llm_summariser_v5.py customer <customer_id> --run-date <YYYY-MM-DD>")
            return

        conn = get_connection()
        cursor = conn.cursor()

        result = process_customer(conn, customer_id, run_date)

        if result:
            # Save to database
            summary = result['summary']
            summary_json = json.dumps(summary, ensure_ascii=False, default=str)

            # Check if update or insert
            cursor.execute("SELECT COUNT(*) FROM dbo.LLM_Customer_Summary WHERE customer_id = ?", customer_id)
            exists = cursor.fetchone()[0] > 0

            is_full_build = result.get('is_full_build', 0)

            # Extract executive summary for rolling_summary_text
            exec_summary = summary.get('executive_summary', '')
            ai_summary = summary.get('ai_summary', '')
            rolling_text = exec_summary or ai_summary or 'No summary available'

            # Get last event timestamp
            last_event_str = summary.get('input_data_summary', {}).get('time_period', {}).get('latest_interaction')
            if last_event_str:
                try:
                    last_event_dt = datetime.fromisoformat(last_event_str.replace('T', ' ').replace('Z', ''))
                except Exception:
                    last_event_dt = datetime.now()
            else:
                last_event_dt = datetime.now()

            if exists:
                cursor.execute("""
                    UPDATE dbo.LLM_Customer_Summary
                    SET summary_json = ?,
                        rolling_summary_text = ?,
                        updated_date = GETDATE(),
                        last_full_build_date = CASE
                            WHEN ? = 1 THEN CAST(GETDATE() AS DATE)
                            ELSE last_full_build_date
                        END
                    WHERE customer_id = ?
                """, summary_json, rolling_text[:4000], is_full_build, customer_id)
            else:
                cursor.execute("""
                    INSERT INTO dbo.LLM_Customer_Summary
                    (customer_id, summary_json, rolling_summary_text, last_event_ts, insert_date, updated_date, last_full_build_date,
                     prompt_version, processing_status, retry_count)
                    VALUES (?, ?, ?, ?, GETDATE(), GETDATE(), CASE WHEN ? = 1 THEN CAST(GETDATE() AS DATE) ELSE NULL END,
                     'v6.0-enterprise-explainable', 'COMPLETED', 0)
                """, customer_id, summary_json, rolling_text[:4000], last_event_dt, is_full_build)

            # Mark as completed
            cursor.execute("EXEC dbo.sp_MarkSummaryCompleted @CustomerID = ?", customer_id)

            conn.commit()
            cursor.close()
            conn.close()

            print("\n" + "=" * 60)
            print("SUMMARY:")
            print("=" * 60)
            # Print summary info (not full JSON to avoid Unicode console issues)
            summary = result['summary']
            dm = summary.get('dashboard_metrics', {})
            health = dm.get('health_score', {}).get('value', 'N/A')
            churn = dm.get('churn_risk', {}).get('score', 'N/A')
            print(f"Customer ID: {summary.get('customer_id')}")
            print(f"Dashboard Metrics: health={health}, churn_risk={churn}")
            print(f"Summary saved to database: LLM_Customer_Summary table")
            print(f"Run 'SELECT summary_json FROM dbo.LLM_Customer_Summary WHERE customer_id = {summary.get('customer_id')}' to view full JSON")
            print("=" * 60)
        else:
            print(f"Failed to process customer {customer_id}")
            cursor.close()
            conn.close()

    else:
        # Process batch
        process_customers(run_date, args.worker_id, args.batch_size)


if __name__ == "__main__":
    main()
