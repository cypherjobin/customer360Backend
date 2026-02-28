# -*- coding: utf-8 -*-
"""
Summary Validator and Reconciliation Layer
==========================================

CRITICAL: This is the FINAL consistency check before any summary is saved.
Runs AFTER LLM generation and Python post-processing, BEFORE database save.

Root Cause Analysis:
-------------------
The pipeline has THREE independent systems generating the same fields:
1. LLM generates: frustration_level, churn_risk_indicator, health_score reasoning
2. Python calculates: frustration_score, churn_risk.score, health_score.value
3. String templates: gating reasons, override_reason text

These systems NEVER cross-check each other, causing contradictions.

The 5 Root Causes:
-----------------
1. LLM generates field that Python also generates → FIXED by single source of truth
2. Python recalculates score but LLM text references old number → FIXED by text replacement
3. Two fields that must agree are set separately → FIXED by cross-field assertions
4. Business rules not enforced in code → FIXED by hardcoded checks
5. Boilerplate LLM text not detected → FIXED by pattern replacement

Usage:
------
from summary_validator import validate_and_reconcile

# After building summary, before saving:
summary_json, report = validate_and_reconcile(summary_json)

if not report.is_valid:
    logger.warning("Validation issues for %s: %s", customer_id, report.summary())

save_to_database(customer_id, summary_json)  # Now clean and consistent

# Check validation metadata
validation_meta = summary_json.get('validation_metadata', {})
print(f"Issues found: {validation_meta['issues_found']}")
print(f"Issues fixed: {validation_meta['issues_fixed']}")
"""

import re
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Any

logger = logging.getLogger(__name__)


class ValidationIssue:
    """Represents a single validation issue."""

    def __init__(self, code: str, severity: str, field: str, description: str,
                 fixed: bool = False, original_value: Any = None, corrected_value: Any = None):
        self.code = code  # e.g., 'FRUSTRATION_MISMATCH', 'TEXT_SCORE_MISMATCH'
        self.severity = severity  # 'CRITICAL', 'HIGH', 'MEDIUM', 'LOW'
        self.field = field  # Field path, e.g., 'sentiment_analysis.frustration_level'
        self.description = description
        self.fixed = fixed
        self.original_value = original_value
        self.corrected_value = corrected_value

    def to_dict(self):
        return {
            'code': self.code,
            'severity': self.severity,
            'field': self.field,
            'description': self.description,
            'fixed': self.fixed,
            'original_value': str(self.original_value)[:100] if self.original_value else None,
            'corrected_value': str(self.corrected_value)[:100] if self.corrected_value else None
        }


class ValidationReport:
    """Validation report with issues and statistics."""

    def __init__(self):
        self.issues: List[ValidationIssue] = []
        self.timestamp = datetime.now().isoformat()

    def add_issue(self, issue: ValidationIssue):
        self.issues.append(issue)

    @property
    def is_valid(self):
        """True if no CRITICAL or HIGH severity unfixed issues."""
        unfixed_critical_high = [
            i for i in self.issues
            if i.severity in ['CRITICAL', 'HIGH'] and not i.fixed
        ]
        return len(unfixed_critical_high) == 0

    @property
    def issues_found(self):
        return len(self.issues)

    @property
    def issues_fixed(self):
        return len([i for i in self.issues if i.fixed])

    @property
    def unfixed_issues(self):
        return [i for i in self.issues if not i.fixed]

    def summary(self):
        """Brief summary of issues."""
        if not self.issues:
            return "No issues found"

        unfixed = self.unfixed_issues
        fixed_count = self.issues_fixed

        if unfixed:
            sev_summary = {}
            for issue in unfixed:
                sev_summary[issue.severity] = sev_summary.get(issue.severity, 0) + 1
            sev_str = ", ".join(f"{count} {sev}" for sev, count in sev_summary.items())
            return f"{fixed_count} fixed, {len(unfixed)} unfixed ({sev_str})"
        else:
            return f"All {fixed_count} issues fixed"

    def to_dict(self):
        return {
            'timestamp': self.timestamp,
            'is_valid': self.is_valid,
            'issues_found': self.issues_found,
            'issues_fixed': self.issues_fixed,
            'unfixed_count': len(self.unfixed_issues),
            'issues': [i.to_dict() for i in self.issues[:20]]  # First 20 issues
        }


def _fix_escalation_risk_vs_threats(summary: Dict, report: ValidationReport):
    """
    FIX: Escalation risk must account for active cancellation threats and switching intent.
    RULE 1: threatened_cancellation=True → escalation_risk must be at least Medium (value >= 0.8)
    RULE 2: switching_intent=True + churn_probability High/Very High → escalation_risk at least Medium
    The current scoring only fires on explicit escalation_threats keywords — misses cancellation signals.
    """
    threats = summary.get('threat_indicators', {})
    dashboard = summary.get('dashboard_metrics', {})
    if not threats or not dashboard:
        return

    escalation = dashboard.get('escalation_risk', {})
    current_value = escalation.get('value', 0)

    threatened_cancellation = threats.get('cancellation_threats', {}).get('threatened_cancellation', False)
    switching_intent = threats.get('competitor_threats', {}).get('switching_intent', False)
    churn_prob = dashboard.get('churn_risk', {}).get('probability', '')

    needs_upgrade = False
    reason = ''

    if threatened_cancellation and current_value < 0.8:
        needs_upgrade = True
        reason = 'threatened_cancellation=True requires escalation_risk >= Medium'

    if switching_intent and churn_prob in ('High', 'Very High') and current_value < 0.8:
        needs_upgrade = True
        reason = f'switching_intent=True with churn_probability={churn_prob} requires escalation_risk >= Medium'

    if needs_upgrade:
        new_value = 0.8
        report.add_issue(ValidationIssue(
            code='ESCALATION_RISK_UNDERRATED',
            severity='HIGH',
            field='dashboard_metrics.escalation_risk',
            description=f'escalation_risk={current_value} (Low) but {reason}. Upgrading to Medium.',
            fixed=True,
            original_value=f'value={current_value}, label={escalation.get("label")}',
            corrected_value='value=0.8, label=Medium'
        ))
        escalation['value'] = new_value
        escalation['label'] = 'Medium'
        escalation['color'] = 'orange'
        # Also update the top-level escalation_risk string field if present
        if 'escalation_risk' in summary and isinstance(summary['escalation_risk'], str):
            summary['escalation_risk'] = 'Medium'


def _fix_switching_intent_as_cancellation(summary: Dict, report: ValidationReport):
    """
    FIX: A port-out inquiry (switching_intent=True away from Virgin) is a form of cancellation.
    If customer is porting OUT to another provider, threatened_cancellation should also be True.
    The pipeline classifies port-out as competitor_threat only — it misses the cancellation dimension.
    """
    threats = summary.get('threat_indicators', {})
    if not threats:
        return

    competitor = threats.get('competitor_threats', {})
    cancellation = threats.get('cancellation_threats', {})

    switching_intent = competitor.get('switching_intent', False)
    threatened_cancellation = cancellation.get('threatened_cancellation', False)

    # Only trigger when switching_intent is True (leaving Virgin) and cancellation not already flagged
    if switching_intent and not threatened_cancellation:
        report.add_issue(ValidationIssue(
            code='SWITCHING_INTENT_IS_CANCELLATION',
            severity='HIGH',
            field='threat_indicators.cancellation_threats.threatened_cancellation',
            description='switching_intent=True (customer leaving Virgin) but threatened_cancellation=False. '
                        'A port-out is a form of cancellation — setting threatened_cancellation=True.',
            fixed=True,
            original_value=False,
            corrected_value=True
        ))
        cancellation['threatened_cancellation'] = True
        if not cancellation.get('cancellation_reason'):
            cancellation['cancellation_reason'] = 'port_out_intent'


def _fix_retention_only_with_no_risk(summary: Dict, report: ValidationReport):
    """
    FIX: priority_focus='RETENTION_ONLY' requires actual retention risk signals.
    If churn_probability is Very Low/Low AND no active (open) issues AND no cancellation/switching threat,
    then RETENTION_ONLY is wrong — should be STANDARD or OPPORTUNITY.

    IMPORTANT: run this BEFORE _fix_retention_consistency so retention_priority
    hasn't been inflated yet by the RETENTION_ONLY focus itself.
    """
    recommended = summary.get('recommended_actions', {})
    priority_focus = recommended.get('priority_focus', '')
    if priority_focus != 'RETENTION_ONLY':
        return

    rrs = summary.get('retention_risk_signals', {})
    risk_factors = rrs.get('risk_factors', [])
    churn_prob = summary.get('dashboard_metrics', {}).get('churn_risk', {}).get('probability', '')

    # Check for active (non-resolved) issues
    key_issues = summary.get('key_issues', [])
    active_issues = [i for i in key_issues
                     if i.get('status', '').upper() not in ('RESOLVED', 'CLOSED')]

    # Check for genuine threat signals
    threats = summary.get('threat_indicators', {})
    threatened_cancellation = threats.get('cancellation_threats', {}).get('threatened_cancellation', False)
    switching_intent = threats.get('competitor_threats', {}).get('switching_intent', False)

    low_risk_churn = churn_prob in ('Very Low', 'Low')
    no_active_issues = not active_issues
    no_risk_factors = not risk_factors
    no_threat_signals = not threatened_cancellation and not switching_intent

    if low_risk_churn and no_active_issues and no_risk_factors and no_threat_signals:
        # Determine correct focus
        profile = summary.get('customer_profile', {})
        marketing_consent = profile.get('marketing_consent', False)
        new_focus = 'OPPORTUNITY' if marketing_consent else 'STANDARD'

        report.add_issue(ValidationIssue(
            code='RETENTION_ONLY_NO_RISK',
            severity='MEDIUM',
            field='recommended_actions.priority_focus',
            description=f'priority_focus=RETENTION_ONLY but churn_probability={churn_prob}, '
                        f'no active issues, no risk_factors, no threat signals. '
                        f'No retention evidence present. Changing to {new_focus}.',
            fixed=True,
            original_value='RETENTION_ONLY',
            corrected_value=new_focus
        ))
        recommended['priority_focus'] = new_focus
        gating = recommended.get('action_gating', {})
        if gating:
            gating['priority_focus'] = new_focus


def _fix_health_label_terminology(summary: Dict, report: ValidationReport):
    """
    FIX: Health score label and reasoning text must use the same canonical term.
    The system generates at least 3 different terms for the 40-54 band:
    'Warning' (label), 'At Risk' (some reasoning), 'AT RISK' (other reasoning).
    Normalise reasoning text to match the label.
    """
    dashboard = summary.get('dashboard_metrics', {})
    health = dashboard.get('health_score', {})
    if not health:
        return

    label = health.get('label', '')
    reasoning = health.get('reasoning', '')

    if not label or not reasoning:
        return

    # Map of alternative terms → canonical label
    alternatives = {
        'Warning': ['at risk', 'at-risk', 'AT RISK', 'At Risk'],
        'At Risk': ['warning', 'Warning', 'WARNING'],
    }

    alt_terms = alternatives.get(label, [])
    for alt in alt_terms:
        if alt in reasoning:
            new_reasoning = reasoning.replace(alt, label)
            report.add_issue(ValidationIssue(
                code='HEALTH_LABEL_TERM_MISMATCH',
                severity='LOW',
                field='dashboard_metrics.health_score.reasoning',
                description=f'Reasoning uses "{alt}" but label="{label}". Normalising to canonical label.',
                fixed=True,
                original_value=alt,
                corrected_value=label
            ))
            health['reasoning'] = new_reasoning
            break


def validate_and_reconcile(summary: Dict) -> Tuple[Dict, ValidationReport]:
    """
    Validate and reconcile a customer summary.

    This is the FINAL consistency check before saving to database.
    Enforces single source of truth, cross-field consistency, and business rules.

    Args:
        summary: The summary JSON dict (will be modified in place)

    Returns:
        Tuple of (cleaned_summary, validation_report)
    """
    report = ValidationReport()

    # Run all validation passes
    _fix_frustration_consistency(summary, report)
    _fix_health_score_consistency(summary, report)
    _fix_health_label_terminology(summary, report)          # NEW: canonical label terms
    _fix_churn_risk_consistency(summary, report)
    _fix_churn_action_consistency(summary, report)
    _fix_services_consistency(summary, report)
    _fix_retention_consistency(summary, report)             # FIXED: now reads retention_risk_signals
    _fix_retention_only_with_no_risk(summary, report)       # NEW: RETENTION_ONLY without evidence
    _fix_switching_intent_as_cancellation(summary, report)  # NEW: port-out = cancellation
    _fix_gating_consistency(summary, report)                # FIXED: +count +bad case_id +NBA +focus sync
    _fix_escalation_risk_vs_threats(summary, report)        # NEW: cancellation/switching → escalation
    _fix_threat_consistency(summary, report)
    _fix_evidence_types(summary, report)
    _detect_boilerplate_text(summary, report)

    # Add validation metadata to summary
    summary['validation_metadata'] = {
        'validated_at': datetime.now().isoformat(),
        'issues_found': report.issues_found,
        'issues_fixed': report.issues_fixed,
        'is_valid': report.is_valid,
        'unfixed_issues': len(report.unfixed_issues)
    }

    return summary, report


# ============================================================================
# ROOT CAUSE #1: Single Source of Truth
# ============================================================================

def _fix_frustration_consistency(summary: Dict, report: ValidationReport):
    """
    ROOT CAUSE #1: LLM generates frustration_level, Python generates frustration_score.
    FIX: Make frustration_score the single source of truth. Derive frustration_level deterministically.
    """
    sentiment = summary.get('sentiment_analysis', {})
    if not sentiment:
        return

    score = sentiment.get('frustration_score')
    level = sentiment.get('frustration_level')

    if score is None:
        return

    # Derive correct level from score using fixed thresholds
    # BUG FIX: Wrong thresholds - should be HIGH:>=60, MEDIUM:>=40, LOW:<40
    if score >= 60:
        correct_level = 'HIGH'
    elif score >= 40:
        correct_level = 'MEDIUM'
    else:
        correct_level = 'LOW'

    if level != correct_level:
        report.add_issue(ValidationIssue(
            code='FRUSTRATION_LEVEL_MISMATCH',
            severity='HIGH',
            field='sentiment_analysis.frustration_level',
            description=f'frustration_level="{level}" but frustration_score={score}. Deriving level from score.',
            fixed=True,
            original_value=level,
            corrected_value=correct_level
        ))
        sentiment['frustration_level'] = correct_level


def _fix_health_score_consistency(summary: Dict, report: ValidationReport):
    """
    ROOT CAUSE #1: LLM generates health_score reasoning with score value, Python calculates actual score.
    ROOT CAUSE #2: Python recalculates score but LLM text still references old number.
    FIX: Make health_score.value the single source of truth. Update text to match.
    """
    dashboard = summary.get('dashboard_metrics', {})
    if not dashboard:
        return

    health = dashboard.get('health_score', {})
    if not health:
        return

    value = health.get('value')
    if value is None:
        return

    # Derive correct label and color from value
    if value >= 80:
        correct_label = 'Excellent'
        correct_color = 'green'
    elif value >= 60:
        correct_label = 'Good'
        correct_color = 'green'
    elif value >= 40:
        correct_label = 'Warning'
        correct_color = 'orange'
    elif value >= 20:
        correct_label = 'At Risk'
        correct_color = 'orange'
    else:
        correct_label = 'Critical'
        correct_color = 'red'

    # Fix label if mismatched
    label = health.get('label')
    if label != correct_label:
        report.add_issue(ValidationIssue(
            code='HEALTH_LABEL_MISMATCH',
            severity='MEDIUM',
            field='dashboard_metrics.health_score.label',
            description=f'health_score.label="{label}" but value={value}. Deriving label from value.',
            fixed=True,
            original_value=label,
            corrected_value=correct_label
        ))
        health['label'] = correct_label

    # Fix color if mismatched
    color = health.get('color')
    if color != correct_color:
        report.add_issue(ValidationIssue(
            code='HEALTH_COLOR_MISMATCH',
            severity='LOW',
            field='dashboard_metrics.health_score.color',
            description=f'health_score.color="{color}" but label={correct_label}. Deriving color.',
            fixed=True,
            original_value=color,
            corrected_value=correct_color
        ))
        health['color'] = correct_color

    # ROOT CAUSE #2: Fix reasoning text that references old score
    reasoning = health.get('reasoning', '')
    if reasoning:
        updated_reasoning = reasoning
        changed = False

        # Pattern 1: N/100 format (e.g. "health score 45/100")
        score_pattern_slash = re.compile(r'\b(\d{1,3})/100\b')
        matches_slash = score_pattern_slash.findall(updated_reasoning)
        for match in matches_slash:
            if int(match) != value:
                updated_reasoning = score_pattern_slash.sub(f'{value}/100', updated_reasoning)
                changed = True
                break

        # Pattern 2: (N) format (e.g. "health score (45)") — stale values in paren format
        # Only replace if the number in parens doesn't match the actual value
        score_pattern_paren = re.compile(r'\((\d{1,3})\)')
        matches_paren = score_pattern_paren.findall(updated_reasoning)
        for match in matches_paren:
            match_int = int(match)
            # Only replace if clearly a score value (0-100 range and differs from actual)
            if 0 <= match_int <= 100 and match_int != value:
                updated_reasoning = score_pattern_paren.sub(f'({value})', updated_reasoning)
                changed = True
                break

        # Pattern 3: Detect "high frustration (0/100)" or "high frustration (0)" where
        # frustration_score context is available — wrong frustration value in health reasoning
        # Pull frustration_score from sentiment_analysis if available
        sentiment = summary.get('sentiment_analysis', {})
        actual_frustration = sentiment.get('frustration_score')
        if actual_frustration is not None:
            # Look for frustration N/100 patterns where N != actual frustration score
            frustration_pattern = re.compile(
                r'(frustration\s*\(?)(\d{1,3})(/100)?(\)?)',
                re.IGNORECASE
            )
            def replace_frustration(m):
                embedded_score = int(m.group(2))
                if embedded_score != actual_frustration:
                    slash_part = m.group(3) or ''
                    return m.group(1) + str(actual_frustration) + slash_part + m.group(4)
                return m.group(0)

            new_reasoning_frust = frustration_pattern.sub(replace_frustration, updated_reasoning)
            if new_reasoning_frust != updated_reasoning:
                updated_reasoning = new_reasoning_frust
                changed = True

        if changed:
            report.add_issue(ValidationIssue(
                code='TEXT_SCORE_MISMATCH',
                severity='MEDIUM',
                field='dashboard_metrics.health_score.reasoning',
                description=f'Reasoning referenced stale score value(s). Corrected to actual values.',
                fixed=True,
                original_value=reasoning[:80],
                corrected_value=updated_reasoning[:80]
            ))
            health['reasoning'] = updated_reasoning


def _fix_churn_risk_consistency(summary: Dict, report: ValidationReport):
    """
    ROOT CAUSE #1: LLM generates churn_risk_indicator, Python calculates churn_risk.score.
    FIX: Make churn_risk.score the single source of truth. Derive indicator deterministically.
    """
    dashboard = summary.get('dashboard_metrics', {})
    if not dashboard:
        return

    churn = dashboard.get('churn_risk', {})
    if not churn:
        return

    score = churn.get('score')
    if score is None:
        return

    # Derive correct probability from score
    if score >= 75:
        correct_prob = 'Very High'
    elif score >= 60:
        correct_prob = 'High'
    elif score >= 40:
        correct_prob = 'Medium'
    elif score >= 20:
        correct_prob = 'Low'
    else:
        correct_prob = 'Very Low'

    # Fix probability if mismatched
    probability = churn.get('probability')
    if probability != correct_prob:
        report.add_issue(ValidationIssue(
            code='CHURN_PROBABILITY_MISMATCH',
            severity='HIGH',
            field='dashboard_metrics.churn_risk.probability',
            description=f'churn_risk.probability="{probability}" but score={score}. Deriving from score.',
            fixed=True,
            original_value=probability,
            corrected_value=correct_prob
        ))
        churn['probability'] = correct_prob

    # Also fix predictive_insights.churn_risk_indicator
    predictive = summary.get('predictive_insights', {})
    if predictive:
        indicator = predictive.get('churn_risk_indicator')

        # BUG FIX #6 & #12: Map score to indicator using standard vocabulary
        # Standard values: {Critical, Elevated, High, Normal} - 'Low' is not standard
        # Critical>=70, Elevated>=50, High>=30, Normal<30
        if score >= 70:
            correct_indicator = 'Critical'
        elif score >= 50:
            correct_indicator = 'Elevated'
        elif score >= 30:
            correct_indicator = 'High'
        else:
            correct_indicator = 'Normal'

        if indicator != correct_indicator:
            report.add_issue(ValidationIssue(
                code='CHURN_INDICATOR_MISMATCH',
                severity='MEDIUM',
                field='predictive_insights.churn_risk_indicator',
                description=f'churn_risk_indicator="{indicator}" but score={score}. Deriving from score.',
                fixed=True,
                original_value=indicator,
                corrected_value=correct_indicator
            ))
            predictive['churn_risk_indicator'] = correct_indicator


# ============================================================================
# ROOT CAUSE #3: Cross-Field Assertions
# ============================================================================

def _fix_retention_consistency(summary: Dict, report: ValidationReport):
    """
    ROOT CAUSE #3: retention_priority and priority_focus are set separately and can contradict.
    FIX: Explicit cross-field assertion. If priority_focus mentions retention, retention_priority must be True.
    BUG FIX: Added check for RETENTION_ONLY, LEGAL_REVIEW_REQUIRED, URGENT_RESOLUTION priority_focus values.

    CRITICAL FIX: retention_priority lives inside retention_risk_signals, NOT at the top level.
    Previous code did summary.get('retention_priority') which always returned None → check never fired.
    """
    # CRITICAL: Read from the correct location — retention_risk_signals
    rrs = summary.get('retention_risk_signals', {})
    retention_priority = rrs.get('retention_priority')
    priority_focus = summary.get('recommended_actions', {}).get('priority_focus', '')
    recommended_action = rrs.get('recommended_action', '')

    # RETENTION-related priority_focus values that require retention_priority=True
    retention_priority_focus_values = ['RETENTION_ONLY', 'LEGAL_REVIEW_REQUIRED', 'URGENT_RESOLUTION']

    # Check for contradiction: retention priority_focus but retention_priority=False
    if retention_priority is False and priority_focus in retention_priority_focus_values:
        report.add_issue(ValidationIssue(
            code='RETENTION_CONTRADICTION',
            severity='HIGH',
            field='retention_risk_signals.retention_priority vs recommended_actions.priority_focus',
            description=f'retention_risk_signals.retention_priority=False but priority_focus="{priority_focus}" requires retention=True.',
            fixed=True,
            original_value=f'retention_priority={retention_priority}, priority_focus={priority_focus}',
            corrected_value=f'Set retention_risk_signals.retention_priority=True to match priority_focus'
        ))
        rrs['retention_priority'] = True
        retention_priority = True  # Update local variable for sync below

    # Also check: retention_priority=False but recommended_action (in retention_risk_signals) mentions retention
    if retention_priority is False and recommended_action:
        recommended_lower = str(recommended_action).lower()
        retention_keywords = ['retention', 'retain', 'churn prevention', 'save customer', 'immediate retention']
        if any(keyword in recommended_lower for keyword in retention_keywords):
            report.add_issue(ValidationIssue(
                code='RETENTION_CONTRADICTION',
                severity='HIGH',
                field='retention_risk_signals.retention_priority vs retention_risk_signals.recommended_action',
                description=f'retention_priority=False but recommended_action="{recommended_action}" mentions retention.',
                fixed=True,
                original_value=f'retention_priority={retention_priority}, recommended_action={recommended_action}',
                corrected_value='Set retention_risk_signals.retention_priority=True to match recommended_action'
            ))
            rrs['retention_priority'] = True
            retention_priority = True

    # BUG FIX #13: Also sync value_at_risk.retention_priority
    # It should mirror retention_priority from retention_risk_signals (authoritative source)
    value_at_risk = summary.get('value_at_risk', {})

    # retention_risk_signals is the single source of truth for retention_priority
    correct_priority = rrs.get('retention_priority', False)

    # Ensure value_at_risk exists
    if not value_at_risk:
        summary['value_at_risk'] = {}
        value_at_risk = summary['value_at_risk']

    value_retention_priority = value_at_risk.get('retention_priority')

    # Sync if null, missing, or different from correct value
    if value_retention_priority is None or value_retention_priority != correct_priority:
        report.add_issue(ValidationIssue(
            code='RETENTION_PRIORITY_SYNC',
            severity='MEDIUM',
            field='value_at_risk.retention_priority',
            description=f'value_at_risk.retention_priority={value_retention_priority} but retention_risk_signals.retention_priority={correct_priority}. Syncing.',
            fixed=True,
            original_value=value_retention_priority,
            corrected_value=correct_priority
        ))
        value_at_risk['retention_priority'] = correct_priority


def _fix_churn_action_consistency(summary: Dict, report: ValidationReport):
    """
    BUG FIX #10: High/Very High churn risk should have proactive recommended_action, not 'Monitor'.
    RULE: If churn_probability is 'High' or 'Very High', recommended_action should be 'Contact' or similar proactive action, not 'Monitor'.
    """
    dashboard = summary.get('dashboard_metrics', {})
    recommended = summary.get('recommended_actions', {})

    if not dashboard or not recommended:
        return

    churn = dashboard.get('churn_risk', {})
    if not churn:
        return

    churn_probability = churn.get('probability', '')
    recommended_action = recommended.get('recommended_action', '')

    # Check if churn is High/Very High but action is passive
    if churn_probability in ['High', 'Very High']:
        recommended_lower = str(recommended_action).lower()
        passive_actions = ['monitor', 'observe', 'watch', 'track', 'follow up', 'no action']

        if any(action in recommended_lower for action in passive_actions):
            # Determine appropriate action based on churn level
            if churn_probability == 'Very High':
                correct_action = 'Immediate Contact'
                urgency = 'urgently'
            else:  # High
                correct_action = 'Contact'
                urgency = 'proactively'

            report.add_issue(ValidationIssue(
                code='CHURN_ACTION_MISMATCH',
                severity='HIGH',
                field='recommended_actions.recommended_action vs dashboard_metrics.churn_risk.probability',
                description=f'churn_probability="{churn_probability}" but recommended_action="{recommended_action}". {churn_probability} churn requires {urgency} contact, not passive monitoring.',
                fixed=True,
                original_value=recommended_action,
                corrected_value=correct_action
            ))
            recommended['recommended_action'] = correct_action
            logger.info(f"  BUG FIX #10: Changed recommended_action from '{recommended_action}' to '{correct_action}' for {churn_probability} churn risk")


def _fix_services_consistency(summary: Dict, report: ValidationReport):
    """
    ROOT CAUSE #3: mobile_active flag and services.status/items are set independently.
    FIX: Cross-field assertion. If mobile_active=True, "Mobile" must be in services.items.
    """
    profile = summary.get('customer_profile', {})
    dashboard = summary.get('dashboard_metrics', {})

    if not profile or not dashboard:
        return

    mobile_active = profile.get('mobile_active')
    services = dashboard.get('services', {})

    if mobile_active is True:
        # Mobile should be in services.items
        items = services.get('items', [])
        if 'Mobile' not in items:
            # Fix: Add Mobile to items
            new_items = ['Mobile'] + items if items else ['Mobile']
            report.add_issue(ValidationIssue(
                code='SERVICES_MOBILE_MISSING',
                severity='MEDIUM',
                field='dashboard_metrics.services.items',
                description=f'mobile_active=True but "Mobile" not in services.items={items}. Adding Mobile.',
                fixed=True,
                original_value=items,
                corrected_value=new_items
            ))
            services['items'] = new_items

            # Also fix status
            if services.get('status') == 'Inactive':
                report.add_issue(ValidationIssue(
                    code='SERVICES_STATUS_MISMATCH',
                    severity='MEDIUM',
                    field='dashboard_metrics.services.status',
                    description=f'mobile_active=True but services.status="Inactive". Changing to Active.',
                    fixed=True,
                    original_value='Inactive',
                    corrected_value='Active'
                ))
                services['status'] = 'Active'
                services['color'] = 'green'


# ============================================================================
# ROOT CAUSE #4: Business Rules Enforcement
# ============================================================================

def _fix_gating_consistency(summary: Dict, report: ValidationReport):
    """
    ROOT CAUSE #4: Business rules not enforced in code.
    RULE 1: GDPR opt-out → safe_to_upsell must be False
    RULE 2: All issues resolved → Don't say "unresolved issues" in reason + fix count
    RULE 3: unresolved_critical_high_count must not count Resolved issues
    RULE 4: bad case_id values (description text instead of INC/Pega ID) → null them
    RULE 5: next_best_action must not recommend upsell if safe_to_upsell=False
    RULE 6: priority_focus must be identical in recommended_actions and action_gating
    """
    profile = summary.get('customer_profile', {})
    recommended = summary.get('recommended_actions', {})
    gating = recommended.get('action_gating', {})

    if not gating:
        return

    # ── RULE 1: GDPR check ───────────────────────────────────────────────────
    marketing_consent = profile.get('marketing_consent') if profile else None
    if marketing_consent is False or marketing_consent is None:
        safe_to_upsell = gating.get('safe_to_upsell')
        if safe_to_upsell is True:
            report.add_issue(ValidationIssue(
                code='GDPR_VIOLATION',
                severity='CRITICAL',
                field='recommended_actions.action_gating.safe_to_upsell',
                description=f'marketing_consent={marketing_consent} but safe_to_upsell=True. GDPR violation.',
                fixed=True,
                original_value=True,
                corrected_value=False
            ))
            gating['safe_to_upsell'] = False

    # ── RULE 2 & 3: Resolved issue checks ───────────────────────────────────
    key_issues = summary.get('key_issues', [])
    if key_issues:
        all_resolved = all(
            issue.get('status', '').upper() in ['RESOLVED', 'CLOSED']
            for issue in key_issues
        )

        if all_resolved:
            # Fix reason text saying "unresolved"
            reason = gating.get('reason', '')
            if 'unresolved' in reason.lower():
                new_reason = re.sub(r'[Uu]nresolved\s+\w+', 'resolved', reason)
                report.add_issue(ValidationIssue(
                    code='GATING_REASON_INACCURATE',
                    severity='MEDIUM',
                    field='recommended_actions.action_gating.reason',
                    description='All issues resolved but gating reason says "unresolved". Fixed.',
                    fixed=True,
                    original_value=reason[:100],
                    corrected_value=new_reason[:100]
                ))
                gating['reason'] = new_reason

            # Fix unresolved_critical_high_count — should be 0 if all resolved
            count = gating.get('unresolved_critical_high_count')
            if count and count > 0:
                report.add_issue(ValidationIssue(
                    code='UNRESOLVED_COUNT_WRONG',
                    severity='HIGH',
                    field='recommended_actions.action_gating.unresolved_critical_high_count',
                    description=f'unresolved_critical_high_count={count} but all key_issues are Resolved. Setting to 0.',
                    fixed=True,
                    original_value=count,
                    corrected_value=0
                ))
                gating['unresolved_critical_high_count'] = 0

    # ── RULE 4: Detect description text used as case_id ─────────────────────
    # A valid case_id matches INC/Pega patterns or is short (< 60 chars with no spaces)
    # Description text is long, contains spaces, and doesn't match case ID patterns
    blocking_issues = gating.get('blocking_issues', [])
    case_id_pattern = re.compile(
        r'^(INC\d+|Pega-\d+|SN-\d+|CAS-\d+|[A-Z]{2,5}\d{4,}|[A-Z]{3}-\d{3,})$',
        re.IGNORECASE
    )
    for blocking_issue in blocking_issues:
        raw_case_id = blocking_issue.get('case_id')
        if raw_case_id and isinstance(raw_case_id, str):
            # Flag if it's a long string with spaces (clearly a description, not an ID)
            if len(raw_case_id) > 40 or ' ' in raw_case_id:
                if not case_id_pattern.match(raw_case_id.strip()):
                    report.add_issue(ValidationIssue(
                        code='BAD_BLOCKING_CASE_ID',
                        severity='MEDIUM',
                        field='recommended_actions.action_gating.blocking_issues[].case_id',
                        description=f'case_id contains description text, not a case ID: "{raw_case_id[:60]}"',
                        fixed=True,
                        original_value=raw_case_id[:60],
                        corrected_value=None
                    ))
                    blocking_issue['case_id'] = None  # Null it — better than wrong data

    # ── RULE 5: next_best_action must not recommend upsell if gated ─────────
    nba = recommended.get('next_best_action', '')
    safe_to_upsell = gating.get('safe_to_upsell', True)
    if not safe_to_upsell and isinstance(nba, str):
        upsell_keywords = ['upgrade', 'upsell', 'offer', 'plan upgrade', 'additional services',
                           'explore', 'promote', 'cross-sell', 'bundle']
        if any(kw in nba.lower() for kw in upsell_keywords):
            # Determine replacement based on gating reason
            blocking_count = gating.get('unresolved_critical_high_count', 0)
            gdpr_block = gating.get('gdpr_block', False)
            if blocking_count and blocking_count > 0:
                safe_nba = f'Resolve {blocking_count} open issue(s) before considering upsell. Focus on customer satisfaction.'
            elif gdpr_block:
                safe_nba = 'Reactive service support only. Customer has opted out of marketing communications — no proactive upsell permitted.'
            else:
                safe_nba = 'Focus on issue resolution and customer satisfaction before considering upsell opportunities.'
            report.add_issue(ValidationIssue(
                code='NEXT_BEST_ACTION_GATING_CONFLICT',
                severity='HIGH',
                field='recommended_actions.next_best_action',
                description=f'next_best_action recommends upsell but safe_to_upsell=False. Replacing with safe action.',
                fixed=True,
                original_value=nba[:80],
                corrected_value=safe_nba[:80]
            ))
            recommended['next_best_action'] = safe_nba

    # ── RULE 6: priority_focus must match between recommended_actions and action_gating ──
    ra_focus = recommended.get('priority_focus', '')
    gating_focus = gating.get('priority_focus', '')
    if ra_focus and gating_focus and ra_focus != gating_focus:
        # Use action_gating.priority_focus as authoritative (it's set by the Python pipeline logic)
        report.add_issue(ValidationIssue(
            code='PRIORITY_FOCUS_MISMATCH',
            severity='HIGH',
            field='recommended_actions.priority_focus vs action_gating.priority_focus',
            description=f'priority_focus mismatch: recommended_actions="{ra_focus}" vs action_gating="{gating_focus}". Syncing to action_gating value.',
            fixed=True,
            original_value=ra_focus,
            corrected_value=gating_focus
        ))
        recommended['priority_focus'] = gating_focus


def _fix_threat_consistency(summary: Dict, report: ValidationReport):
    """
    ROOT CAUSE #4: Business rules not enforced.
    RULE 1: threatened_cancellation=True → churn_probability must be Very High
    RULE 2: threatened_cancellation=True → priority_focus should be RETENTION_ONLY
    """
    threats = summary.get('threat_indicators', {})
    if not threats:
        return

    cancellation = threats.get('cancellation_threats', {})
    threatened = cancellation.get('threatened_cancellation', False)

    if threatened:
        # RULE 1: Check churn probability
        dashboard = summary.get('dashboard_metrics', {})
        churn = dashboard.get('churn_risk', {}) if dashboard else {}
        churn_prob = churn.get('probability', '')

        if churn_prob not in ['Very High', 'High']:
            report.add_issue(ValidationIssue(
                code='CANCELLATION_CHURN_RISK',
                severity='CRITICAL',
                field='dashboard_metrics.churn_risk.probability',
                description=f'threatened_cancellation=True but churn_probability="{churn_prob}". Should be Very High.',
                fixed=True,
                original_value=churn_prob,
                corrected_value='Very High'
            ))
            churn['probability'] = 'Very High'
            # Also update score
            churn['score'] = max(churn.get('score', 0), 75)

        # RULE 2: Check priority_focus
        recommended = summary.get('recommended_actions', {})
        priority_focus = recommended.get('priority_focus', '')
        if priority_focus not in ['RETENTION_ONLY', 'LEGAL_REVIEW_REQUIRED', 'URGENT_RESOLUTION']:
            report.add_issue(ValidationIssue(
                code='CANCELLATION_PRIORITY_FOCUS',
                severity='HIGH',
                field='recommended_actions.priority_focus',
                description=f'threatened_cancellation=True but priority_focus="{priority_focus}". Should be RETENTION_ONLY.',
                fixed=True,
                original_value=priority_focus,
                corrected_value='RETENTION_ONLY'
            ))
            if 'recommended_actions' not in summary:
                summary['recommended_actions'] = {}
            summary['recommended_actions']['priority_focus'] = 'RETENTION_ONLY'


# ============================================================================
# ROOT CAUSE #5: Boilerplate Text Detection
# ============================================================================

def _detect_boilerplate_text(summary: Dict, report: ValidationReport):
    """
    ROOT CAUSE #5: LLM boilerplate fallback text not detected.
    FIX: Detect known boilerplate phrases and replace with factual summary.
    """
    dashboard = summary.get('dashboard_metrics', {})
    if not dashboard:
        return

    health = dashboard.get('health_score', {})
    reasoning = health.get('reasoning', '')

    if not reasoning:
        return

    value = health.get('value', 100)
    label = health.get('label', 'Unknown')

    # Known boilerplate patterns that shouldn't appear for non-healthy scores
    boilerplate_patterns = [
        ('within normal range', lambda v, l: l in ['Warning', 'At Risk', 'Critical']),
        ('no immediate concerns', lambda v, l: l in ['Warning', 'At Risk', 'Critical']),
        ('No key issues', lambda v, l: l in ['Warning', 'At Risk', 'Critical']),
        ('no data quality warnings', lambda v, l: False),  # Never appropriate
    ]

    for pattern, should_fix in boilerplate_patterns:
        if pattern.lower() in reasoning.lower():
            if should_fix(value, label):
                # Replace with factual summary
                new_reasoning = (
                    f"Customer health score is {value}/100 ({label}). "
                )

                if label == 'Warning':
                    new_reasoning += "Warning indicators detected - attention needed to prevent escalation."
                elif label == 'At Risk':
                    new_reasoning += "Significant concerns exist - immediate intervention recommended."
                elif label == 'Critical':
                    new_reasoning += "Critical issues detected - urgent action required."

                report.add_issue(ValidationIssue(
                    code='BOILERPLATE_TEXT_DETECTED',
                    severity='MEDIUM',
                    field='dashboard_metrics.health_score.reasoning',
                    description=f'Boilerplate "{pattern}" found with {label} score. Replaced with factual summary.',
                    fixed=True,
                    original_value=reasoning[:100],
                    corrected_value=new_reasoning[:100]
                ))
                health['reasoning'] = new_reasoning
                break


# ============================================================================
# Type Consistency
# ============================================================================

def _fix_evidence_types(summary: Dict, report: ValidationReport):
    """
    Fix mixed types in evidence arrays.
    All evidence items should be objects with {quote, interaction_id, date} structure.
    """
    threats = summary.get('threat_indicators', {})
    if not threats:
        return

    for threat_type in ['cancellation_threats', 'escalation_threats', 'regulatory_threats', 'legal_threats']:
        if threat_type in threats:
            evidence = threats[threat_type].get('evidence', [])
            if evidence:
                # Check for mixed types
                has_strings = any(isinstance(item, str) for item in evidence)

                if has_strings:
                    # Standardize to all objects
                    standardized = []
                    for item in evidence:
                        if isinstance(item, str):
                            standardized.append({'quote': item})
                        elif isinstance(item, dict):
                            standardized.append(item)
                        else:
                            standardized.append({'quote': str(item)})

                    report.add_issue(ValidationIssue(
                        code='EVIDENCE_TYPE_MISMATCH',
                        severity='LOW',
                        field=f'threat_indicators.{threat_type}.evidence',
                        description=f'Mixed types in evidence array. Standardized to all objects.',
                        fixed=True,
                        original_value=f'{len([i for i in evidence if isinstance(i, str)])} strings',
                        corrected_value='All objects'
                    ))
                    threats[threat_type]['evidence'] = standardized
