"""
VIBO SQL Tools
==============
Structured data retrieval functions for the VIBO agent.
Each function accepts a customer_id and returns a dict that the LLM
can use to formulate a response.

All queries are parameterised to prevent SQL injection.
All results are scoped to a single customer_id.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Optional
from vibo_database import db_cursor

logger = logging.getLogger("vibo.tools.sql")


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL 1: get_account_summary
# ═══════════════════════════════════════════════════════════════════════════════
def get_account_summary(customer_id: str) -> dict:
    """
    Retrieve the full AI-generated customer summary including health score,
    churn risk, CES, sentiment, agent briefing, and recommended actions.
    
    Source: LLM_Customer_Summary.summary_json
    """
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT 
                customer_id,
                rolling_summary_text,
                summary_json,
                updated_date,
                processing_status,
                last_full_build_date
            FROM LLM_Customer_Summary
            WHERE customer_id = ?
        """, customer_id)
        
        row = cursor.fetchone()
        if not row:
            return {
                "found": False,
                "customer_id": customer_id,
                "message": "No AI summary available for this customer."
            }
        
        # Parse the rich JSON summary
        summary_data = {}
        if row.summary_json:
            try:
                summary_data = json.loads(row.summary_json)
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON in summary for {customer_id}")
        
        return {
            "found": True,
            "customer_id": customer_id,
            "summary_text": row.rolling_summary_text or "",
            "summary_data": summary_data,
            "last_updated": row.updated_date.isoformat() if row.updated_date else None,
            "processing_status": row.processing_status,
            "last_full_build": str(row.last_full_build_date) if row.last_full_build_date else None,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL 2: get_open_cases
# ═══════════════════════════════════════════════════════════════════════════════
def get_open_cases(customer_id: str, include_resolved: bool = False) -> dict:
    """
    Get open (and optionally resolved) cases from Pega and ServiceNow.
    
    Source: Customer360_Events (filtered by source_system)
    Also pulls from LLM_Customer_Summary.summary_json for pre-computed case lists.
    """
    # First, try the pre-computed case list from the summary JSON
    summary = get_account_summary(customer_id)
    cases_from_summary = []
    
    if summary.get("found") and summary.get("summary_data"):
        sd = summary["summary_data"]
        open_cases = sd.get("open_cases", [])
        resolved_cases = sd.get("resolved_cases", [])
        
        if include_resolved:
            cases_from_summary = open_cases + resolved_cases
        else:
            cases_from_summary = open_cases
    
    # Also query raw events for most up-to-date data
    status_filter = ""
    if not include_resolved:
        status_filter = """
            AND (
                JSON_VALUE(event_detail_json, '$.status') NOT IN ('Closed', 'Resolved', 'Cancelled')
                OR JSON_VALUE(event_detail_json, '$.status') IS NULL
            )
        """
    
    with db_cursor() as cursor:
        cursor.execute(f"""
            SELECT 
                natural_key,
                source_system,
                event_type,
                event_timestamp,
                event_detail_json
            FROM Customer360_Events
            WHERE customer_id = ?
              AND source_system IN ('Pega', 'ServiceNow')
              AND (is_deleted = 0 OR is_deleted IS NULL)
              {status_filter}
            ORDER BY event_timestamp DESC
        """, customer_id)
        
        raw_cases = []
        for row in cursor.fetchall():
            detail = {}
            if row.event_detail_json:
                try:
                    detail = json.loads(row.event_detail_json)
                except json.JSONDecodeError:
                    pass
            
            raw_cases.append({
                "case_id": row.natural_key,
                "source_system": row.source_system,
                "event_type": row.event_type,
                "timestamp": row.event_timestamp.isoformat() if row.event_timestamp else None,
                "status": detail.get("status", "Unknown"),
                "type": detail.get("type", detail.get("case_type", "")),
                "sub_type": detail.get("sub_type", ""),
                "assigned_to": detail.get("assigned_to", detail.get("assignment_group", "")),
                "priority": detail.get("priority", ""),
                "description": detail.get("description", detail.get("short_description", ""))[:200],
            })
    
    # Also extract SLA info from summary
    sla_info = {}
    if summary.get("found") and summary.get("summary_data"):
        sd = summary["summary_data"]
        sla_info = {
            "sla_breaches": sd.get("sla_breaches", 0),
            "sla_breach_risk": sd.get("sla_breach_risk", "Unknown"),
        }
    
    return {
        "customer_id": customer_id,
        "open_cases_count": len([c for c in raw_cases if c["status"] not in ("Closed", "Resolved", "Cancelled")]),
        "total_cases_returned": len(raw_cases),
        "include_resolved": include_resolved,
        "cases_from_summary": cases_from_summary,
        "cases_from_events": raw_cases,
        "sla_info": sla_info,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL 3: get_recent_calls
# ═══════════════════════════════════════════════════════════════════════════════
def get_recent_calls(customer_id: str, limit: int = 5, days_back: int = 30) -> dict:
    """
    Retrieve recent call recordings/transcripts with AI summaries,
    detected issues, root causes, and customer quotes.
    
    Source: CallTranscript
    """
    cutoff = datetime.now() - timedelta(days=days_back)
    
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT TOP (?)
                transcript_id,
                call_start,
                call_end,
                call_summary,
                call_segment,
                call_product,
                call_issues_json,
                call_root_causes_json,
                customer_quotes_json
            FROM CallTranscript
            WHERE customer_id = ?
              AND call_start >= ?
            ORDER BY call_start DESC
        """, limit, customer_id, cutoff)
        
        calls = []
        for row in cursor.fetchall():
            # Parse JSON fields safely
            issues = _safe_json_parse(row.call_issues_json, [])
            root_causes = _safe_json_parse(row.call_root_causes_json, [])
            quotes = _safe_json_parse(row.customer_quotes_json, [])
            
            duration_mins = None
            if row.call_start and row.call_end:
                duration_mins = round((row.call_end - row.call_start).total_seconds() / 60, 1)
            
            calls.append({
                "transcript_id": row.transcript_id,
                "call_start": row.call_start.isoformat() if row.call_start else None,
                "call_end": row.call_end.isoformat() if row.call_end else None,
                "duration_minutes": duration_mins,
                "summary": row.call_summary or "",
                "segment": row.call_segment or "",
                "product": row.call_product or "",
                "issues": issues,
                "root_causes": root_causes,
                "customer_quotes": quotes,
            })
    
    return {
        "customer_id": customer_id,
        "total_calls_returned": len(calls),
        "days_back": days_back,
        "calls": calls,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL 4: get_revenue_and_products
# ═══════════════════════════════════════════════════════════════════════════════
def get_revenue_and_products(customer_id: str) -> dict:
    """
    Get revenue, product portfolio, contract dates, tenure,
    service status, and revenue segmentation.
    
    Source: Revenue_Cache
    """
    with db_cursor() as cursor:
        cursor.execute("""
            SELECT 
                customer_id,
                customer_type,
                has_mobile,
                has_fixed,
                mobile_account,
                fixed_account,
                mobile_active,
                fixed_active,
                service_status,
                product_list,
                monthly_revenue_mobile,
                monthly_revenue_fixed,
                monthly_revenue_total,
                annual_revenue_total,
                revenue_segment,
                contract_end_fixed,
                tenure_months,
                plan_count,
                account_category,
                device_count,
                device_financing_revenue,
                cached_at
            FROM Revenue_Cache
            WHERE customer_id = ?
        """, customer_id)
        
        row = cursor.fetchone()
        if not row:
            return {
                "found": False,
                "customer_id": customer_id,
                "message": "No revenue data found for this customer."
            }
        
        return {
            "found": True,
            "customer_id": customer_id,
            "customer_type": row.customer_type,
            "services": {
                "has_mobile": bool(row.has_mobile),
                "has_fixed": bool(row.has_fixed),
                "mobile_active": bool(row.mobile_active),
                "fixed_active": bool(row.fixed_active),
                "mobile_account": row.mobile_account,
                "fixed_account": row.fixed_account,
                "service_status": row.service_status,
                "product_list": row.product_list,
            },
            "revenue": {
                "monthly_mobile": float(row.monthly_revenue_mobile or 0),
                "monthly_fixed": float(row.monthly_revenue_fixed or 0),
                "monthly_total": float(row.monthly_revenue_total or 0),
                "annual_total": float(row.annual_revenue_total or 0),
                "revenue_segment": row.revenue_segment,
                "device_financing": float(row.device_financing_revenue or 0),
            },
            "contract": {
                "contract_end_fixed": str(row.contract_end_fixed) if row.contract_end_fixed else None,
                "tenure_months": row.tenure_months,
            },
            "account": {
                "plan_count": row.plan_count,
                "account_category": row.account_category,
                "device_count": row.device_count,
            },
            "cache_freshness": row.cached_at.isoformat() if row.cached_at else None,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL 5: get_device_portfolio
# ═══════════════════════════════════════════════════════════════════════════════
def get_device_portfolio(customer_id: str, active_only: bool = False) -> dict:
    """
    Get all devices associated with the customer including brand, model,
    contract status, installment details, and MIC.
    
    Source: Customer_Device_Assets
    """
    active_filter = "AND is_contract_active = 1" if active_only else ""
    
    with db_cursor() as cursor:
        cursor.execute(f"""
            SELECT 
                device_id,
                device_brand,
                device_model,
                device_colour,
                device_memory,
                device_value,
                down_payment,
                installment_count,
                installment_amount,
                installments_remaining,
                contract_start_date,
                contract_end_date,
                contract_status,
                device_status,
                package_code,
                package_name,
                imei,
                sim_serial_number,
                mic_monthly,
                is_contract_active
            FROM Customer_Device_Assets
            WHERE customer_id = ?
            {active_filter}
            ORDER BY contract_start_date DESC
        """, customer_id)
        
        devices = []
        total_mic = 0.0
        active_contracts = 0
        expired_contracts = 0
        
        for row in cursor.fetchall():
            is_active = bool(row.is_contract_active)
            mic = float(row.mic_monthly or 0)
            
            if is_active:
                active_contracts += 1
                total_mic += mic
            else:
                expired_contracts += 1
            
            devices.append({
                "device_id": row.device_id,
                "brand": row.device_brand,
                "model": row.device_model,
                "colour": row.device_colour,
                "memory": row.device_memory,
                "device_value": float(row.device_value or 0),
                "down_payment": float(row.down_payment or 0),
                "installment_amount": float(row.installment_amount or 0),
                "installments_remaining": row.installments_remaining,
                "installment_count": row.installment_count,
                "contract_start": str(row.contract_start_date) if row.contract_start_date else None,
                "contract_end": str(row.contract_end_date) if row.contract_end_date else None,
                "contract_status": row.contract_status,
                "device_status": row.device_status,
                "package_name": row.package_name,
                # Note: IMEI and SIM are sensitive - included but guardrails may mask them
                "imei": row.imei,
                "sim_serial_number": row.sim_serial_number,
                "mic_monthly": mic,
                "is_active": is_active,
            })
    
    return {
        "customer_id": customer_id,
        "total_devices": len(devices),
        "active_contracts": active_contracts,
        "expired_contracts": expired_contracts,
        "total_mic_monthly": round(total_mic, 2),
        "device_summary": f"{active_contracts} active, {expired_contracts} expired, \u20AC{total_mic:.2f}/month MIC",
        "active_only_filter": active_only,
        "devices": devices,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL 6: get_interactions
# ═══════════════════════════════════════════════════════════════════════════════
def get_interactions(customer_id: str, days_back: int = 30,
                     source_system: Optional[str] = None) -> dict:
    """
    Get all customer interactions across all source systems.
    
    Source: Customer360_Events
    """
    cutoff = datetime.now() - timedelta(days=days_back)
    
    params = [customer_id, cutoff]
    source_filter = ""
    if source_system:
        source_filter = "AND source_system = ?"
        params.append(source_system)
    
    with db_cursor() as cursor:
        cursor.execute(f"""
            SELECT 
                natural_key,
                source_system,
                event_type,
                event_timestamp,
                event_detail_json,
                transcript_id
            FROM Customer360_Events
            WHERE customer_id = ?
              AND event_timestamp >= ?
              AND (is_deleted = 0 OR is_deleted IS NULL)
              {source_filter}
            ORDER BY event_timestamp DESC
        """, *params)
        
        interactions = []
        source_counts = {}
        
        for row in cursor.fetchall():
            detail = _safe_json_parse(row.event_detail_json, {})
            source = row.source_system or "Unknown"
            source_counts[source] = source_counts.get(source, 0) + 1
            
            interactions.append({
                "event_key": row.natural_key,
                "source_system": source,
                "event_type": row.event_type,
                "timestamp": row.event_timestamp.isoformat() if row.event_timestamp else None,
                "detail": _summarise_event_detail(detail),
                "transcript_id": row.transcript_id,
            })
    
    return {
        "customer_id": customer_id,
        "total_interactions": len(interactions),
        "days_back": days_back,
        "source_filter": source_system,
        "by_source": source_counts,
        "interactions": interactions,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL 7: get_risk_assessment
# ═══════════════════════════════════════════════════════════════════════════════
def get_risk_assessment(customer_id: str) -> dict:
    """
    Comprehensive risk assessment pulling health score, churn risk,
    CES, escalation risk, and SLA status from the pre-computed summary.
    
    Source: LLM_Customer_Summary.summary_json (enrichment fields)
    """
    summary = get_account_summary(customer_id)
    
    if not summary.get("found"):
        return {
            "found": False,
            "customer_id": customer_id,
            "message": "No risk assessment available - no AI summary exists."
        }
    
    sd = summary.get("summary_data", {})
    av = sd.get("account_value", {})
    
    return {
        "found": True,
        "customer_id": customer_id,
        "health": {
            "score": sd.get("health_score"),
            "band": sd.get("health_band"),
        },
        "churn": {
            "risk": sd.get("churn_risk", "Unknown"),
            "indicators": sd.get("churn_risk_indicators", []),
        },
        "escalation": {
            "risk": sd.get("escalation_risk"),
            "score": sd.get("escalation_risk_score"),
            "reason": sd.get("escalation_risk_reason"),
        },
        "customer_effort": {
            "score": sd.get("customer_effort_score"),
            "band": sd.get("customer_effort_band"),
        },
        "sentiment": {
            "current": sd.get("sentiment"),
            "reason": sd.get("sentiment_reason"),
        },
        "sla": {
            "breaches": sd.get("sla_breaches", 0),
            "breach_risk": sd.get("sla_breach_risk"),
        },
        "resolution_status": sd.get("resolution_status"),
        "revenue_segment": av.get("revenue_segment"),
        "is_repeat_caller": sd.get("is_repeat_caller", False),
        "recommended_actions": sd.get("recommended_actions", []),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL 8: get_contact_timeline
# ═══════════════════════════════════════════════════════════════════════════════
def get_contact_timeline(customer_id: str) -> dict:
    """
    Get the chronological contact timeline from the pre-computed summary.
    
    Source: LLM_Customer_Summary.summary_json → contact_timeline
    """
    summary = get_account_summary(customer_id)
    
    if not summary.get("found"):
        return {
            "found": False,
            "customer_id": customer_id,
            "message": "No timeline available."
        }
    
    sd = summary.get("summary_data", {})
    
    return {
        "found": True,
        "customer_id": customer_id,
        "total_contacts_30d": sd.get("total_contacts_30d", 0),
        "timeline": sd.get("contact_timeline", []),
        "call_intents": sd.get("call_intents_summary", []),
        "interactions_summary": sd.get("interactions_summary", ""),
        "pega_summary": sd.get("pega_cases_summary", ""),
        "servicenow_summary": sd.get("servicenow_summary", ""),
    }


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def _safe_json_parse(text, default=None):
    """Safely parse JSON string, return default on failure."""
    if not text:
        return default
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return default


def _summarise_event_detail(detail: dict, max_len: int = 300) -> str:
    """Extract a readable summary from event detail JSON."""
    if not detail:
        return ""
    
    # Try common fields in order of preference
    for field in ["description", "short_description", "summary", "notes", 
                  "wrap_up_comment", "agent_notes", "resolution_notes"]:
        if detail.get(field):
            text = str(detail[field])
            return text[:max_len] + "..." if len(text) > max_len else text
    
    # Fallback: stringify the dict (truncated)
    text = str(detail)
    return text[:max_len] + "..." if len(text) > max_len else text


# ═══════════════════════════════════════════════════════════════════════════════
# TOOL REGISTRY (maps tool names to functions)
# ═══════════════════════════════════════════════════════════════════════════════
SQL_TOOLS = {
    "get_account_summary":    get_account_summary,
    "get_open_cases":         get_open_cases,
    "get_recent_calls":       get_recent_calls,
    "get_revenue_and_products": get_revenue_and_products,
    "get_device_portfolio":   get_device_portfolio,
    "get_interactions":       get_interactions,
    "get_risk_assessment":    get_risk_assessment,
    "get_contact_timeline":   get_contact_timeline,
}


if __name__ == "__main__":
    # Quick test with a sample customer
    import sys
    cid = sys.argv[1] if len(sys.argv) > 1 else "10900099"
    print(f"\n{'='*60}")
    print(f"Testing SQL tools for customer: {cid}")
    print(f"{'='*60}")
    
    for name, func in SQL_TOOLS.items():
        print(f"\n--- {name} ---")
        try:
            result = func(cid)
            # Print a compact summary
            if isinstance(result, dict):
                for k, v in result.items():
                    if isinstance(v, (list, dict)) and len(str(v)) > 200:
                        print(f"  {k}: [{type(v).__name__} with {len(v)} items]")
                    else:
                        print(f"  {k}: {v}")
            print(f"  ✓ OK")
        except Exception as e:
            print(f"  ✗ Error: {e}")
