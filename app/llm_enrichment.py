"""
Customer 360 - Deterministic Enrichment Layer (Phase 2)
=======================================================
Rule-based enrichment that runs AFTER Phase 1 guardrails validation.

All enrichment is:
- Deterministic (same inputs = same outputs)
- Rule-based (no LLM, no probability)
- Fully auditable
- Explainable

Enrichment Modules:
1. SLA Tracking - Days unresolved, breach flags, risk levels
2. Customer Effort Score (CES) - 0-10 score based on contacts
3. Friction Flags - Billing, cancellation, repeat intent, high value unresolved
4. Compliance Flags - Deceased, regulatory risk, complaints
5. Customer Health Score - 0-100 weighted composite score
6. Sentiment Confidence - Based on data source quality
7. Churn Risk Indicators (Phase 2.1) - 12 deterministic risk indicators
8. Enrichment Metadata - Version tracking

Author: Data Engineering Team
Date: 2026-02-17
Version: 2.2 (Added Device Financing & Contract End Indicators)
"""

import json
import logging
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURATION
# ============================================================

SLA_THRESHOLDS = {
    'critical_days': 7,      # Days after which open cases are critical
    'warning_days': 3,       # Days after which breach risk is medium
    'complaint_days': 2,     # SLA for complaint cases
}

CES_WEIGHTS = {
    'contact_frequency': 0.30,   # High contact count = higher effort
    'repeat_caller': 0.25,       # Repeat caller penalty
    'unresolved_issues': 0.25,   # Unresolved cases increase effort
    'open_case_count': 0.20,     # More open cases = higher effort
}

HEALTH_SCORE_WEIGHTS = {
    'revenue_segment': 0.20,     # High value = lower risk (reduced from 0.25)
    'payment_risk': 0.15,        # Payment issues increase risk (reduced from 0.20)
    'contact_frequency': 0.15,   # High contacts = higher risk
    'resolution_status': 0.15,   # Unresolved = higher risk (reduced from 0.20)
    'sentiment': 0.15,           # Negative sentiment = higher risk (reduced from 0.20)
    'tenure': 0.20,              # Long tenure = lower risk (NEW)
}

FRICTION_PATTERNS = {
    'billing_keywords': ['bill', 'invoice', 'payment', 'charge', 'refund', 'direct debit'],
    'cancellation_keywords': ['cancel', 'close account', 'terminate', 'disconnect'],
    'complaint_keywords': ['complain', 'complaint', 'escalate', 'unhappy', 'dissatisfied'],
}

# ============================================================
# SLA TRACKING
# ============================================================

def calculate_sla_metrics(summary_json, events_data, run_date=None):
    """
    Calculate SLA metrics for open cases.

    Returns:
        sla_metrics: dict with keys:
            - days_unresolved (int): Maximum days any case has been open
            - sla_breach_flag (bool): True if any case > critical_days
            - breach_risk_level (str): 'Low', 'Medium', 'High'
            - open_cases_breached (int): Count of cases in breach
    """
    if run_date is None:
        run_date = date.today()

    # Parse summary JSON
    try:
        data = json.loads(summary_json) if isinstance(summary_json, str) else summary_json
    except:
        return {
            'days_unresolved': 0,
            'sla_breach_flag': False,
            'breach_risk_level': 'Low',
            'open_cases_breached': 0
        }

    open_cases = data.get('open_cases', [])
    if not open_cases:
        return {
            'days_unresolved': 0,
            'sla_breach_flag': False,
            'breach_risk_level': 'Low',
            'open_cases_breached': 0
        }

    max_days = 0
    breached_count = 0
    warning_count = 0

    for case in open_cases:
        created_str = case.get('created_date', '')
        if not created_str:
            continue

        try:
            # Parse date (handle various formats)
            if isinstance(created_str, str):
                created_date = datetime.strptime(created_str[:10], '%Y-%m-%d').date()
            elif isinstance(created_str, datetime):
                created_date = created_str.date()
            elif isinstance(created_str, date):
                created_date = created_str
            else:
                continue

            days_open = (run_date - created_date).days
            if days_open > max_days:
                max_days = days_open

            # Check against thresholds
            if days_open > SLA_THRESHOLDS['critical_days']:
                breached_count += 1
            elif days_open > SLA_THRESHOLDS['warning_days']:
                warning_count += 1

        except (ValueError, TypeError) as e:
            logger.warning(f"Could not parse date for SLA: {created_str}")
            continue

    # Determine breach risk level
    if breached_count > 0:
        risk_level = 'High'
        breach_flag = True
    elif warning_count > 0:
        risk_level = 'Medium'
        breach_flag = False
    else:
        risk_level = 'Low'
        breach_flag = False

    return {
        'days_unresolved': max_days,
        'sla_breach_flag': breach_flag,
        'breach_risk_level': risk_level,
        'open_cases_breached': breached_count
    }


# ============================================================
# CUSTOMER EFFORT SCORE (CES)
# ============================================================

def calculate_ces(summary_json):
    """
    Calculate Customer Effort Score (0-10).

    Higher score = Higher customer effort (worse experience).

    Scoring:
    - 0-2: Very Low Effort
    - 3-4: Low Effort
    - 5-6: Medium Effort
    - 7-8: High Effort
    - 9-10: Very High Effort

    Returns:
        ces_score: dict with score (0-10), band, and breakdown
    """
    try:
        data = json.loads(summary_json) if isinstance(summary_json, str) else summary_json
    except:
        return {
            'score': 0,
            'band': 'Unknown',
            'breakdown': {
                'contact_frequency_score': 0,
                'repeat_caller_score': 0,
                'unresolved_issues_score': 0,
                'open_case_count_score': 0
            }
        }

    # Component 1: Contact Frequency (0-10)
    total_contacts = data.get('total_contacts_30d', 0)
    if total_contacts == 0:
        contact_score = 0
    elif total_contacts <= 2:
        contact_score = 2
    elif total_contacts <= 5:
        contact_score = 5
    elif total_contacts <= 10:
        contact_score = 7
    else:
        contact_score = 10

    # Component 2: Repeat Caller (0-10)
    is_repeat = data.get('is_repeat_caller', False)
    repeat_score = 8 if is_repeat else 0

    # Component 3: Unresolved Issues (0-10)
    resolution = data.get('resolution_status', '').lower()
    open_cases = len(data.get('open_cases', []))

    if 'unresolved' in resolution or 'ongoing' in resolution:
        if open_cases >= 3:
            unresolved_score = 10
        elif open_cases >= 2:
            unresolved_score = 7
        elif open_cases >= 1:
            unresolved_score = 5
        else:
            unresolved_score = 3
    elif 'partial' in resolution:
        unresolved_score = 4
    else:
        unresolved_score = 0

    # Component 4: Open Case Count (0-10)
    if open_cases == 0:
        case_score = 0
    elif open_cases == 1:
        case_score = 3
    elif open_cases == 2:
        case_score = 6
    elif open_cases == 3:
        case_score = 8
    else:
        case_score = 10

    # Calculate weighted score
    weighted_score = (
        contact_score * CES_WEIGHTS['contact_frequency'] +
        repeat_score * CES_WEIGHTS['repeat_caller'] +
        unresolved_score * CES_WEIGHTS['unresolved_issues'] +
        case_score * CES_WEIGHTS['open_case_count']
    )

    # Determine band
    if weighted_score <= 2:
        band = 'Very Low Effort'
    elif weighted_score <= 4:
        band = 'Low Effort'
    elif weighted_score <= 6:
        band = 'Medium Effort'
    elif weighted_score <= 8:
        band = 'High Effort'
    else:
        band = 'Very High Effort'

    return {
        'score': round(weighted_score, 1),
        'band': band,
        'breakdown': {
            'contact_frequency_score': contact_score,
            'repeat_caller_score': repeat_score,
            'unresolved_issues_score': unresolved_score,
            'open_case_count_score': case_score
        }
    }


# ============================================================
# FRICTION FLAGS
# ============================================================

def detect_friction_flags(summary_json, events_data):
    """
    Detect friction points in customer journey.

    Returns:
        friction_flags: dict with boolean flags
    """
    try:
        data = json.loads(summary_json) if isinstance(summary_json, str) else summary_json
    except:
        return {
            'billing_error_flag': False,
            'cancellation_request_flag': False,
            'repeat_same_intent_flag': False,
            'high_value_unresolved_flag': False
        }

    flags = {
        'billing_error_flag': False,
        'cancellation_request_flag': False,
        'repeat_same_intent_flag': False,
        'high_value_unresolved_flag': False
    }

    # Check billing issues in key_issues and interactions
    key_issues = data.get('key_issues', [])
    contact_timeline = data.get('contact_timeline', [])

    # Billing error detection
    billing_keywords = FRICTION_PATTERNS['billing_keywords']
    for issue in key_issues:
        issue_text = str(issue.get('issue', '')).lower()
        if any(kw in issue_text for kw in billing_keywords):
            flags['billing_error_flag'] = True
            break

    # Cancellation request detection
    cancel_keywords = FRICTION_PATTERNS['cancellation_keywords']
    for issue in key_issues:
        issue_text = str(issue.get('issue', '')).lower()
        if any(kw in issue_text for kw in cancel_keywords):
            flags['cancellation_request_flag'] = True
            break

    # Detect repeat same intent from call_intents_summary
    call_intents = data.get('call_intents_summary', [])
    intent_counts = {}
    for intent in call_intents:
        # Handle both dict and string formats
        if isinstance(intent, dict):
            intent_name = intent.get('intent', 'Unknown')
            occurrences = intent.get('occurrences', 0)
        elif isinstance(intent, str):
            intent_name = intent
            occurrences = 1
        else:
            continue
        if intent_name != 'Unknown':
            intent_counts[intent_name] = intent_counts.get(intent_name, 0) + occurrences

    # Flag if same intent appears 3+ times
    for intent_name, count in intent_counts.items():
        if count >= 3:
            flags['repeat_same_intent_flag'] = True
            break

    # High value unresolved detection
    account_value = data.get('account_value', {})
    revenue_segment = account_value.get('revenue_segment', 'Unknown')
    resolution = data.get('resolution_status', '').lower()

    is_high_value = revenue_segment in ['High Value', 'Medium Value']
    is_unresolved = any(word in resolution for word in ['unresolved', 'ongoing', 'partial'])

    if is_high_value and is_unresolved:
        flags['high_value_unresolved_flag'] = True

    return flags


# ============================================================
# COMPLIANCE FLAGS
# ============================================================

def detect_compliance_flags(summary_json, events_data):
    """
    Detect compliance and regulatory concerns.

    Returns:
        compliance_flags: dict with boolean flags
    """
    try:
        data = json.loads(summary_json) if isinstance(summary_json, str) else summary_json
    except:
        return {
            'deceased_customer_flag': False,
            'regulatory_risk_flag': False,
            'complaint_case_flag': False
        }

    flags = {
        'deceased_customer_flag': False,
        'regulatory_risk_flag': False,
        'complaint_case_flag': False
    }

    # Check for deceased indicators
    complaint_keywords = FRICTION_PATTERNS['complaint_keywords']
    contact_timeline = data.get('contact_timeline', [])
    key_issues = data.get('key_issues', [])

    # Deceased detection (specific keywords)
    deceased_keywords = ['deceased', 'passed away', 'death', 'bereavement', 'executor']
    for issue in key_issues:
        issue_text = str(issue.get('issue', '')).lower()
        if any(kw in issue_text for kw in deceased_keywords):
            flags['deceased_customer_flag'] = True
            break

    # Regulatory risk (complaints, escalations)
    complaint_count = 0
    for issue in key_issues:
        issue_text = str(issue.get('issue', '')).lower()
        if any(kw in issue_text for kw in complaint_keywords):
            complaint_count += 1

    if complaint_count >= 2:
        flags['complaint_case_flag'] = True

    # Check events for regulatory cases
    for event in events_data:
        source = event.get('source_system', '')
        detail = event.get('detail', {})

        # Check for regulatory case types in Pega
        if source == 'PegaCase':
            case_type = str(detail.get('case_type', '')).lower()
            if any(kw in case_type for kw in complaint_keywords):
                flags['regulatory_risk_flag'] = True

    return flags


# ============================================================
# CUSTOMER HEALTH SCORE
# ============================================================

def calculate_health_score(summary_json):
    """
    Calculate Customer Health Score (0-100).

    Higher score = Healthier customer.
    Lower score = Higher risk.

    Components:
    - Revenue Segment (0-100): High value = 100, Low value = 0
    - Payment Risk (0-100): No issues = 100, billing errors = 0
    - Contact Frequency (0-100): 0-2 contacts = 100, 10+ = 0
    - Resolution Status (0-100): Fully resolved = 100, Unresolved = 0
    - Sentiment (0-100): Positive = 100, Negative = 0

    Returns:
        health_score: dict with score (0-100), band, and breakdown
    """
    try:
        data = json.loads(summary_json) if isinstance(summary_json, str) else summary_json
    except:
        return {
            'score': 50,
            'band': 'Unknown',
            'breakdown': {
                'revenue_segment_score': 50,
                'payment_risk_score': 50,
                'contact_frequency_score': 50,
                'resolution_status_score': 50,
                'sentiment_score': 50,
                'tenure_score': 50
            }
        }

    # Component 1: Revenue Segment (0-100)
    account_value = data.get('account_value', {})
    revenue_segment = account_value.get('revenue_segment', 'Unknown')

    if revenue_segment == 'High Value':
        revenue_score = 100
    elif revenue_segment == 'Medium Value':
        revenue_score = 70
    elif revenue_segment == 'Low Value':
        revenue_score = 40
    else:
        revenue_score = 50

    # Component 2: Payment Risk (0-100)
    # Check for billing flags in key_issues
    key_issues = data.get('key_issues', [])
    billing_keywords = FRICTION_PATTERNS['billing_keywords']
    has_billing_issues = False

    for issue in key_issues:
        issue_text = str(issue.get('issue', '')).lower()
        if any(kw in issue_text for kw in billing_keywords):
            has_billing_issues = True
            break

    payment_score = 40 if has_billing_issues else 100

    # Component 3: Contact Frequency (0-100)
    total_contacts = data.get('total_contacts_30d', 0)
    if total_contacts <= 2:
        contact_score = 100
    elif total_contacts <= 5:
        contact_score = 80
    elif total_contacts <= 10:
        contact_score = 50
    else:
        contact_score = 20

    # Component 4: Resolution Status (0-100)
    resolution = data.get('resolution_status', '').lower()
    if 'resolved' in resolution and 'full' in resolution:
        resolution_score = 100
    elif 'partial' in resolution:
        resolution_score = 60
    elif 'unresolved' in resolution or 'ongoing' in resolution:
        resolution_score = 20
    else:
        resolution_score = 50

    # Component 5: Sentiment (0-100)
    sentiment = data.get('sentiment', 'Unknown').lower()
    if sentiment == 'positive':
        sentiment_score = 100
    elif sentiment == 'neutral':
        sentiment_score = 60
    elif sentiment == 'mixed':
        sentiment_score = 40
    elif sentiment == 'negative':
        sentiment_score = 10
    else:
        sentiment_score = 50

    # Component 6: Tenure (0-100)
    # Long-tenured customers are more loyal and less likely to churn
    tenure_months = account_value.get('tenure_months')
    if tenure_months:
        try:
            tenure = int(tenure_months)
            if tenure >= 60:  # 5+ years = very loyal
                tenure_score = 100
            elif tenure >= 36:  # 3+ years = loyal
                tenure_score = 85
            elif tenure >= 24:  # 2+ years = established
                tenure_score = 70
            elif tenure >= 12:  # 1+ year = stable
                tenure_score = 55
            elif tenure >= 6:   # 6+ months = new but stable
                tenure_score = 40
            else:  # < 6 months = very new
                tenure_score = 25
        except:
            tenure_score = 50
    else:
        tenure_score = 50  # Unknown tenure = neutral

    # Calculate weighted score
    weighted_score = (
        revenue_score * HEALTH_SCORE_WEIGHTS['revenue_segment'] +
        payment_score * HEALTH_SCORE_WEIGHTS['payment_risk'] +
        contact_score * HEALTH_SCORE_WEIGHTS['contact_frequency'] +
        resolution_score * HEALTH_SCORE_WEIGHTS['resolution_status'] +
        sentiment_score * HEALTH_SCORE_WEIGHTS['sentiment'] +
        tenure_score * HEALTH_SCORE_WEIGHTS['tenure']
    )

    # Determine band
    if weighted_score >= 80:
        band = 'Healthy'
    elif weighted_score >= 60:
        band = 'Stable'
    elif weighted_score >= 40:
        band = 'At Risk'
    else:
        band = 'Critical'

    return {
        'score': round(weighted_score, 0),
        'band': band,
        'breakdown': {
            'revenue_segment_score': revenue_score,
            'payment_risk_score': payment_score,
            'contact_frequency_score': contact_score,
            'resolution_status_score': resolution_score,
            'sentiment_score': sentiment_score,
            'tenure_score': tenure_score
        }
    }


# ============================================================
# SENTIMENT CONFIDENCE
# ============================================================

def calculate_sentiment_confidence(summary_json):
    """
    Calculate confidence in sentiment assessment.

    Based on data source quality:
    - High: Has verified customer_quotes
    - Medium: Has agent wrap-up comments but no quotes
    - Low: Minimal data

    Returns:
        sentiment_confidence: dict with level and reason
    """
    try:
        data = json.loads(summary_json) if isinstance(summary_json, str) else summary_json
    except:
        return {
            'level': 'Low',
            'reason': 'No data available'
        }

    customer_voice = data.get('customer_voice', [])
    contact_timeline = data.get('contact_timeline', [])
    total_contacts = data.get('total_contacts_30d', 0)

    # Check for verified quotes
    has_quotes = len(customer_voice) > 0

    # Check for wrap-up comments
    has_wrapup = False
    for contact in contact_timeline:
        summary = contact.get('summary', '')
        if summary and summary.lower() not in ['n/a', 'no details', 'no details recorded', '']:
            has_wrapup = True
            break

    # Determine confidence
    if has_quotes:
        level = 'High'
        reason = f'{len(customer_voice)} verified customer quotes available'
    elif has_wrapup:
        level = 'Medium'
        reason = 'Agent wrap-up comments available, no verified quotes'
    elif total_contacts > 0:
        level = 'Low'
        reason = 'Contact data available but no qualitative feedback'
    else:
        level = 'Unknown'
        reason = 'No contact data available'

    return {
        'level': level,
        'reason': reason,
        'quote_count': len(customer_voice),
        'contact_count': total_contacts
    }


# ============================================================
# CHURN RISK INDICATORS (Phase 2.1)
# ============================================================

def calculate_churn_risk_indicators(summary_json, events_data):
    """
    Calculate churn risk exposure indicators using deterministic rules.

    This does NOT calculate churn probability. It identifies risk factors
    that indicate elevated churn exposure.

    Returns:
        churn_risk: dict with:
            - indicators: dict of triggered indicators with explanations
            - indicator_count: total number of triggered indicators
            - churn_exposure_level: 'Low', 'Moderate', 'High'
            - risk_trajectory: 'Rising', 'Stable', 'Falling', 'Unknown'
    """
    try:
        data = json.loads(summary_json) if isinstance(summary_json, str) else summary_json
    except:
        return {
            'indicators': {},
            'indicator_count': 0,
            'churn_exposure_level': 'Unknown',
            'risk_trajectory': 'Unknown'
        }

    indicators = {}

    # Indicator 1: High value + unresolved issues
    account_value = data.get('account_value', {})
    revenue_segment = account_value.get('revenue_segment', 'Unknown')
    resolution = data.get('resolution_status', '').lower()
    open_cases = len(data.get('open_cases', []))

    is_high_value = revenue_segment in ['High Value', 'Medium Value']
    is_unresolved = any(word in resolution for word in ['unresolved', 'ongoing', 'partial'])

    if is_high_value and is_unresolved:
        indicators['high_value_unresolved'] = {
            'triggered': True,
            'explanation': f'{revenue_segment} customer with {resolution} status and {open_cases} open case(s)',
            'severity': 'HIGH' if revenue_segment == 'High Value' else 'MEDIUM'
        }

    # Indicator 2: Multiple cancellation requests
    contact_timeline = data.get('contact_timeline', [])
    key_issues = data.get('key_issues', [])

    cancel_keywords = ['cancel', 'close account', 'terminate', 'disconnect', 'leave']
    cancellation_count = 0

    for issue in key_issues:
        issue_text = str(issue.get('issue', '')).lower()
        if any(kw in issue_text for kw in cancel_keywords):
            cancellation_count += 1

    if cancellation_count >= 2:
        indicators['multiple_cancellation_requests'] = {
            'triggered': True,
            'explanation': f'{cancellation_count} distinct mentions of cancellation',
            'severity': 'HIGH'
        }
    elif cancellation_count == 1:
        indicators['cancellation_mention'] = {
            'triggered': True,
            'explanation': 'Single cancellation request detected',
            'severity': 'MEDIUM'
        }

    # Indicator 3: Negative sentiment + high contact frequency
    sentiment = data.get('sentiment', 'Unknown').lower()
    total_contacts = data.get('total_contacts_30d', 0)

    is_negative = sentiment in ['negative', 'mixed']
    is_high_frequency = total_contacts >= 5

    if is_negative and is_high_frequency:
        indicators['negative_sentiment_high_contacts'] = {
            'triggered': True,
            'explanation': f'{sentiment} sentiment with {total_contacts} contacts in 30 days',
            'severity': 'HIGH' if total_contacts >= 8 else 'MEDIUM'
        }

    # Indicator 4: Billing errors + high value customer
    billing_keywords = ['bill', 'invoice', 'payment', 'charge', 'refund', 'direct debit', 'overcharge']
    has_billing_issues = False

    for issue in key_issues:
        issue_text = str(issue.get('issue', '')).lower()
        if any(kw in issue_text for kw in billing_keywords):
            has_billing_issues = True
            break

    if has_billing_issues and is_high_value:
        indicators['billing_issues_high_value'] = {
            'triggered': True,
            'explanation': f'Billing-related issues for {revenue_segment} customer',
            'severity': 'HIGH'
        }

    # Indicator 5: Contract ending soon + unresolved issues
    contract_end = account_value.get('contract_end_fixed_adjusted') or account_value.get('contract_end_fixed')
    is_contract_ending_soon = False

    if contract_end:
        try:
            if isinstance(contract_end, str):
                contract_date = datetime.strptime(contract_end[:10], '%Y-%m-%d').date()
            else:
                contract_date = contract_end

            days_to_end = (contract_date - date.today()).days
            is_contract_ending_soon = days_to_end <= 60 and days_to_end >= 0
        except:
            pass

    if is_contract_ending_soon and is_unresolved:
        indicators['contract_ending_with_issues'] = {
            'triggered': True,
            'explanation': f'Contract ending soon with {resolution} status',
            'severity': 'HIGH'
        }

    # Indicator 6: Low health score
    # Note: This uses the health score calculated earlier
    # We'll calculate it inline here for independence
    health_score_estimate = 50  # default
    if revenue_segment == 'High Value':
        health_score_estimate = 70
    elif revenue_segment == 'Low Value':
        health_score_estimate = 40

    if is_unresolved:
        health_score_estimate -= 30
    if is_negative:
        health_score_estimate -= 20
    if total_contacts > 8:
        health_score_estimate -= 15

    if health_score_estimate < 40:
        indicators['low_health_score'] = {
            'triggered': True,
            'explanation': f'Estimated health score {health_score_estimate} indicates elevated risk',
            'severity': 'HIGH' if health_score_estimate < 30 else 'MEDIUM'
        }

    # Indicator 7: High Customer Effort Score
    # Calculate CES inline
    contact_score = 5 if total_contacts >= 5 else 2
    repeat_score = 8 if data.get('is_repeat_caller') else 0
    unresolved_score = 5 if is_unresolved else 0
    case_score = 3 if open_cases >= 1 else 0

    ces_estimate = (contact_score * 0.30 + repeat_score * 0.25 +
                   unresolved_score * 0.25 + case_score * 0.20) * 10

    if ces_estimate >= 7:
        indicators['high_customer_effort'] = {
            'triggered': True,
            'explanation': f'Estimated CES {ces_estimate:.1f}/10 indicates high customer effort',
            'severity': 'HIGH'
        }

    # Indicator 8: SLA breach + high value
    # Check for SLA breaches from events
    has_sla_breach = False
    days_unresolved = 0

    for event in events_data:
        if event.get('source_system') in ['PegaCase', 'SNOWCase']:
            status = event.get('event_status', '').upper()
            created = event.get('event_timestamp')

            if created and 'OPEN' in status:
                try:
                    if isinstance(created, str):
                        created_date = datetime.strptime(created[:10], '%Y-%m-%d').date()
                    else:
                        created_date = created

                    days_open = (date.today() - created_date).days
                    if days_open > days_unresolved:
                        days_unresolved = days_open

                    if days_open > 7:
                        has_sla_breach = True

                except:
                    pass

    if has_sla_breach and is_high_value:
        indicators['sla_breach_high_value'] = {
            'triggered': True,
            'explanation': f'{revenue_segment} customer with case(s) unresolved for {days_unresolved}+ days',
            'severity': 'HIGH'
        }

    # Indicator 9: Cross-product risk
    customer_type = account_value.get('customer_type', '')
    is_cross_product = customer_type == 'Mobile + Fixed'

    if is_cross_product and is_unresolved:
        # Check if both services have issues
        mobile_active = account_value.get('mobile_active', True)
        fixed_active = account_value.get('fixed_active', True)

        if not mobile_active and not fixed_active:
            indicators['cross_product_risk'] = {
                'triggered': True,
                'explanation': 'Both mobile and fixed services showing issues - full account at risk',
                'severity': 'HIGH'
            }
        elif not mobile_active or not fixed_active:
            indicators['cross_product_risk'] = {
                'triggered': True,
                'explanation': 'Multi-product customer with service issues - churn risk elevated',
                'severity': 'MEDIUM'
            }

    # Indicator 10: Competitor mentions
    competitor_keywords = ['sky', 'eir', 'vodafone', 'three', 'toshiba', 'switching', 'competitor', 'other provider']
    has_competitor_mention = False

    for contact in contact_timeline:
        summary = str(contact.get('summary', '')).lower()
        if any(kw in summary for kw in competitor_keywords):
            has_competitor_mention = True
            break

    if has_competitor_mention:
        indicators['competitor_mention'] = {
            'triggered': True,
            'explanation': 'Competitor or switching mentioned in interactions',
            'severity': 'MEDIUM'
        }

    # Indicator 11: Device contracts ending soon (within 60 days)
    devices = account_value.get('devices', [])
    devices_ending_soon = []
    active_device_count = 0

    for device in devices:
        if device.get('is_contract_active') and device.get('contract_end_date'):
            active_device_count += 1
            try:
                contract_date = datetime.strptime(device['contract_end_date'], '%Y-%m-%d').date()
                days_to_end = (contract_date - date.today()).days
                if days_to_end <= 60 and days_to_end >= 0:
                    devices_ending_soon.append({
                        'device': f"{device.get('brand', '')} {device.get('model', '')}",
                        'days_remaining': days_to_end
                    })
            except:
                pass

    if devices_ending_soon:
        device_list = ', '.join([d['device'] for d in devices_ending_soon[:3]])
        indicators['device_contracts_ending'] = {
            'triggered': True,
            'explanation': f'{len(devices_ending_soon)} device contract(s) ending within 60 days: {device_list}',
            'severity': 'MEDIUM'
        }

    # Indicator 12: High device financing revenue + unresolved issues
    # Customers with high MIC (€50+/month) have significant device financing exposure
    device_financing_revenue = account_value.get('device_financing_revenue')
    if device_financing_revenue:
        try:
            # Handle both string "€50.00" and float formats
            if isinstance(device_financing_revenue, str):
                mic_value = float(device_financing_revenue.replace('€', '').replace(',', '').strip())
            else:
                mic_value = float(device_financing_revenue)
        except:
            mic_value = 0

        # High MIC threshold: €50/month indicates significant device financing
        if mic_value >= 50 and is_unresolved:
            indicators['high_device_financing_risk'] = {
                'triggered': True,
                'explanation': f'€{mic_value:.0f}/month device financing at risk with unresolved issues',
                'severity': 'HIGH'
            }
        elif mic_value >= 100 and is_negative:
            # Very high MIC with negative sentiment is critical
            indicators['high_device_financing_risk'] = {
                'triggered': True,
                'explanation': f'€{mic_value:.0f}/month device financing exposure with negative sentiment',
                'severity': 'HIGH'
            }

    # Count high vs medium severity indicators
    high_severity_count = sum(1 for ind in indicators.values() if ind.get('severity') == 'HIGH')
    medium_severity_count = sum(1 for ind in indicators.values() if ind.get('severity') == 'MEDIUM')

    # Determine churn exposure level
    # High: 2+ HIGH severity OR 3+ total indicators
    # Moderate: 1 HIGH severity OR 2 MEDIUM severity
    # Low: 0-1 indicators total
    indicator_count = len(indicators)

    if high_severity_count >= 2 or indicator_count >= 3:
        churn_exposure = 'High'
    elif high_severity_count >= 1 or medium_severity_count >= 2:
        churn_exposure = 'Moderate'
    elif indicator_count >= 1:
        churn_exposure = 'Low'
    else:
        churn_exposure = 'Low'

    # Determine risk trajectory
    # Rising: Negative indicators + high contact frequency
    # Stable: Mixed or neutral indicators
    # Falling: Positive indicators + low contact frequency
    if is_negative and is_high_frequency and indicator_count >= 2:
        risk_trajectory = 'Rising'
    elif sentiment == 'positive' and total_contacts <= 2 and indicator_count <= 1:
        risk_trajectory = 'Falling'
    elif indicator_count == 0:
        risk_trajectory = 'Stable'
    elif is_negative:
        risk_trajectory = 'Rising'
    else:
        risk_trajectory = 'Stable'

    return {
        'indicators': indicators,
        'indicator_count': indicator_count,
        'high_severity_count': high_severity_count,
        'medium_severity_count': medium_severity_count,
        'churn_exposure_level': churn_exposure,
        'risk_trajectory': risk_trajectory
    }


# ============================================================
# MAIN ENRICHMENT FUNCTION
# ============================================================

def enrich_summary(summary_json, events_data, run_date=None):
    """
    Apply all enrichment modules to a validated summary.

    This runs AFTER Phase 1 guardrails validation.
    All enrichment is deterministic and rule-based.

    Args:
        summary_json: Validated summary JSON (string or dict)
        events_data: List of event dicts for the customer
        run_date: Date for SLA calculations (default: today)

    Returns:
        enriched_summary_json: String JSON with enrichment appended
        enrichment_report: Dict with all enrichment values
    """
    if run_date is None:
        run_date = date.today()

    # Parse summary
    try:
        data = json.loads(summary_json) if isinstance(summary_json, str) else summary_json
    except:
        logger.error("Invalid summary JSON for enrichment")
        return summary_json, {}

    # Run all enrichment modules
    enrichment = {
        'sla_tracking': calculate_sla_metrics(summary_json, events_data, run_date),
        'customer_effort_score': calculate_ces(summary_json),
        'friction_flags': detect_friction_flags(summary_json, events_data),
        'compliance_flags': detect_compliance_flags(summary_json, events_data),
        'health_score': calculate_health_score(summary_json),
        'sentiment_confidence': calculate_sentiment_confidence(summary_json),
        'churn_risk': calculate_churn_risk_indicators(summary_json, events_data),
    }

    # Add metadata
    enrichment['metadata'] = {
        'phase_version': '2.1',  # Updated for churn risk extension
        'scoring_version': 'v1.0',
        'enrichment_timestamp': datetime.now().isoformat(),
        'sla_thresholds_used': SLA_THRESHOLDS,
    }

    # Append to summary (don't modify existing fields)
    data['enrichment'] = enrichment

    # Return as JSON string
    enriched_json = json.dumps(data, ensure_ascii=False, default=str)

    return enriched_json, enrichment


# ============================================================
# ENRICHMENT REPORT FORMATTER
# ============================================================

def format_enrichment_report(enrichment):
    """Format enrichment data for logging."""
    lines = []
    lines.append("=== ENRICHMENT SUMMARY ===")

    sla = enrichment.get('sla_tracking', {})
    lines.append(f"SLA: {sla.get('breach_risk_level', 'Unknown')} risk | "
                f"{sla.get('days_unresolved', 0)} days unresolved | "
                f"{sla.get('open_cases_breached', 0)} cases breached")

    ces = enrichment.get('customer_effort_score', {})
    lines.append(f"CES: {ces.get('score', 0)}/10 ({ces.get('band', 'Unknown')})")

    health = enrichment.get('health_score', {})
    lines.append(f"Health: {health.get('score', 0)}/100 ({health.get('band', 'Unknown')})")

    friction = enrichment.get('friction_flags', {})
    active_friction = [k for k, v in friction.items() if v]
    if active_friction:
        lines.append(f"Friction: {', '.join(active_friction)}")
    else:
        lines.append("Friction: None detected")

    compliance = enrichment.get('compliance_flags', {})
    active_compliance = [k for k, v in compliance.items() if v]
    if active_compliance:
        lines.append(f"Compliance: {', '.join(active_compliance)}")
    else:
        lines.append("Compliance: No concerns")

    sentiment = enrichment.get('sentiment_confidence', {})
    lines.append(f"Sentiment Confidence: {sentiment.get('level', 'Unknown')}")

    # Churn risk (Phase 2.1)
    churn = enrichment.get('churn_risk', {})
    lines.append(f"Churn Exposure: {churn.get('churn_exposure_level', 'Unknown')} | "
                f"Trajectory: {churn.get('risk_trajectory', 'Unknown')} | "
                f"Indicators: {churn.get('indicator_count', 0)}")

    return ' | '.join(lines)


# ============================================================
# EXPORT FUNCTIONS
# ============================================================

__all__ = [
    'enrich_summary',
    'format_enrichment_report',
    'calculate_sla_metrics',
    'calculate_ces',
    'detect_friction_flags',
    'detect_compliance_flags',
    'calculate_health_score',
    'calculate_sentiment_confidence',
    'calculate_churn_risk_indicators',
]
