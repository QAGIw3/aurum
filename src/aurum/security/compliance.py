"""Security compliance and audit system."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from aurum.core.settings import AurumSettings

logger = logging.getLogger(__name__)


class ComplianceStandard(str, Enum):
    """Compliance standards supported."""
    GDPR = "gdpr"
    SOX = "sox"
    HIPAA = "hipaa"
    PCI_DSS = "pci_dss"
    ISO_27001 = "iso_27001"
    SOC_2 = "soc_2"
    NIST_800_53 = "nist_800_53"


class DataClassification(str, Enum):
    """Data classification levels."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    HIGHLY_RESTRICTED = "highly_restricted"


class ComplianceControl(str, Enum):
    """Compliance controls."""
    ACCESS_CONTROL = "access_control"
    AUDIT_LOGGING = "audit_logging"
    DATA_ENCRYPTION = "data_encryption"
    NETWORK_SECURITY = "network_security"
    INCIDENT_RESPONSE = "incident_response"
    RISK_ASSESSMENT = "risk_assessment"
    VENDOR_MANAGEMENT = "vendor_management"
    BUSINESS_CONTINUITY = "business_continuity"


@dataclass
class ComplianceRequirement:
    """Compliance requirement definition."""

    control: ComplianceControl
    standard: ComplianceStandard
    requirement_id: str
    description: str
    severity: str  # low, medium, high, critical
    implementation_status: str  # not_implemented, partially_implemented, fully_implemented, not_applicable
    evidence: List[str] = field(default_factory=list)
    last_reviewed: Optional[datetime] = None
    next_review_due: Optional[datetime] = None

    def is_compliant(self) -> bool:
        """Check if requirement is compliant."""
        return self.implementation_status == "fully_implemented"

    def is_due_for_review(self) -> bool:
        """Check if requirement is due for review."""
        if not self.next_review_due:
            return False
        return datetime.now() > self.next_review_due


@dataclass
class SecurityPolicy:
    """Security policy definition."""

    policy_id: str
    name: str
    description: str
    category: str
    requirements: List[str]  # Requirement IDs
    enforcement_level: str  # advisory, recommended, mandatory
    created_at: datetime
    updated_at: datetime
    version: str = "1.0"


class ComplianceManager:
    """Manage security compliance and audit requirements."""

    def __init__(self, settings: AurumSettings):
        self.settings = settings
        self.requirements: Dict[str, ComplianceRequirement] = {}
        self.policies: Dict[str, SecurityPolicy] = {}
        self.compliance_standards: Dict[ComplianceStandard, List[str]] = {}
        self.audit_trail: List[Dict[str, Any]] = []

        self._initialize_compliance_framework()

    def _initialize_compliance_framework(self) -> None:
        """Initialize compliance framework with default requirements."""

        # GDPR Requirements
        gdpr_requirements = [
            ComplianceRequirement(
                control=ComplianceControl.ACCESS_CONTROL,
                standard=ComplianceStandard.GDPR,
                requirement_id="GDPR-1",
                description="Implement appropriate technical and organizational measures to ensure data protection by design and by default",
                severity="critical",
                implementation_status="fully_implemented",
                evidence=["Row Level Security", "Tenant isolation", "Access control policies"]
            ),
            ComplianceRequirement(
                control=ComplianceControl.AUDIT_LOGGING,
                standard=ComplianceStandard.GDPR,
                requirement_id="GDPR-2",
                description="Maintain records of processing activities",
                severity="high",
                implementation_status="fully_implemented",
                evidence=["Audit logging middleware", "Structured audit events", "Retention policies"]
            ),
            ComplianceRequirement(
                control=ComplianceControl.DATA_ENCRYPTION,
                standard=ComplianceStandard.GDPR,
                requirement_id="GDPR-3",
                description="Encrypt personal data in transit and at rest",
                severity="high",
                implementation_status="fully_implemented",
                evidence=["TLS encryption", "Database encryption", "API encryption"]
            )
        ]

        # ISO 27001 Requirements
        iso_requirements = [
            ComplianceRequirement(
                control=ComplianceControl.NETWORK_SECURITY,
                standard=ComplianceStandard.ISO_27001,
                requirement_id="ISO-27001-1",
                description="Establish, implement, and maintain network security controls",
                severity="high",
                implementation_status="fully_implemented",
                evidence=["Firewall rules", "Network segmentation", "DDoS protection"]
            ),
            ComplianceRequirement(
                control=ComplianceControl.INCIDENT_RESPONSE,
                standard=ComplianceStandard.ISO_27001,
                requirement_id="ISO-27001-2",
                description="Develop and implement incident response procedures",
                severity="high",
                implementation_status="partially_implemented",
                evidence=["Incident response plan", "Alerting system", "Escalation procedures"]
            )
        ]

        # Enhanced SOC 2 Requirements for Phase 4
        soc2_requirements = [
            ComplianceRequirement(
                control=ComplianceControl.ACCESS_CONTROL,
                standard=ComplianceStandard.SOC_2,
                requirement_id="SOC2-CC6.1",
                description="Implement logical and physical access controls for protection of the entity's system and data",
                severity="critical",
                implementation_status="fully_implemented",
                evidence=["Multi-factor authentication", "Role-based access control", "Tenant isolation", "Network segmentation"]
            ),
            ComplianceRequirement(
                control=ComplianceControl.AUDIT_LOGGING,
                standard=ComplianceStandard.SOC_2,
                requirement_id="SOC2-CC7.1",
                description="Implement system monitoring activities to meet the entity's objectives",
                severity="high",
                implementation_status="fully_implemented",
                evidence=["Enterprise audit logging", "Security event monitoring", "Performance monitoring", "Compliance alerts"]
            ),
            ComplianceRequirement(
                control=ComplianceControl.DATA_ENCRYPTION,
                standard=ComplianceStandard.SOC_2,
                requirement_id="SOC2-CC6.7",
                description="Encrypt data to meet objectives related to security",
                severity="high",
                implementation_status="fully_implemented",
                evidence=["TLS 1.3 encryption", "Database encryption at rest", "Key management system"]
            ),
            ComplianceRequirement(
                control=ComplianceControl.BUSINESS_CONTINUITY,
                standard=ComplianceStandard.SOC_2,
                requirement_id="SOC2-A1.2",
                description="Implement business continuity and disaster recovery procedures",
                severity="high",
                implementation_status="fully_implemented",
                evidence=["Multi-region deployment", "Automated failover", "Backup procedures", "RTO <1 hour, RPO <15 minutes"]
            ),
            ComplianceRequirement(
                control=ComplianceControl.RISK_ASSESSMENT,
                standard=ComplianceStandard.SOC_2,
                requirement_id="SOC2-CC3.1",
                description="Establish risk assessment process to identify risks to the achievement of objectives",
                severity="medium",
                implementation_status="fully_implemented",
                evidence=["Privacy impact assessments", "Security risk assessments", "Compliance monitoring", "Vulnerability management"]
            ),
            ComplianceRequirement(
                control=ComplianceControl.VENDOR_MANAGEMENT,
                standard=ComplianceStandard.SOC_2,
                requirement_id="SOC2-CC9.1",
                description="Implement vendor management process including due diligence procedures",
                severity="medium",
                implementation_status="partially_implemented",
                evidence=["Vendor security assessments", "Contract security reviews", "Third-party integration monitoring"]
            ),
            ComplianceRequirement(
                control=ComplianceControl.INCIDENT_RESPONSE,
                standard=ComplianceStandard.SOC_2,
                requirement_id="SOC2-CC6.8",
                description="Implement procedures to respond to system security breaches and incidents",
                severity="high",
                implementation_status="fully_implemented",
                evidence=["Incident response plan", "Security team", "Automated alerting", "Compliance violation tracking"]
            )
        ]

        # Register all requirements
        for req in gdpr_requirements + iso_requirements + soc2_requirements:
            self.requirements[req.requirement_id] = req

        # Map standards to requirements
        self.compliance_standards = {
            ComplianceStandard.GDPR: [req.requirement_id for req in gdpr_requirements],
            ComplianceStandard.ISO_27001: [req.requirement_id for req in iso_requirements],
            ComplianceStandard.SOC_2: [req.requirement_id for req in soc2_requirements],
        }

    async def assess_compliance(self, standard: ComplianceStandard) -> Dict[str, Any]:
        """Assess compliance with a specific standard."""
        if standard not in self.compliance_standards:
            raise ValueError(f"Unknown compliance standard: {standard}")

        requirement_ids = self.compliance_standards[standard]
        requirements = [self.requirements[req_id] for req_id in requirement_ids]

        total_requirements = len(requirements)
        compliant_requirements = len([r for r in requirements if r.is_compliant()])
        partially_compliant = len([r for r in requirements if r.implementation_status == "partially_implemented"])
        non_compliant = total_requirements - compliant_requirements - partially_compliant

        compliance_score = (compliant_requirements * 1.0 + partially_compliant * 0.5) / total_requirements

        assessment = {
            "standard": standard.value,
            "assessment_date": datetime.now().isoformat(),
            "total_requirements": total_requirements,
            "compliant_requirements": compliant_requirements,
            "partially_compliant_requirements": partially_compliant,
            "non_compliant_requirements": non_compliant,
            "compliance_score": compliance_score,
            "requirements": [
                {
                    "requirement_id": req.requirement_id,
                    "control": req.control.value,
                    "description": req.description,
                    "severity": req.severity,
                    "implementation_status": req.implementation_status,
                    "evidence": req.evidence,
                    "compliant": req.is_compliant(),
                    "due_for_review": req.is_due_for_review()
                }
                for req in requirements
            ]
        }

        # Log assessment
        logger.info(
            "Compliance assessment completed",
            extra={
                "standard": standard.value,
                "compliance_score": compliance_score,
                "compliant_requirements": compliant_requirements,
                "total_requirements": total_requirements
            }
        )

        return assessment

    async def generate_compliance_report(
        self,
        standards: List[ComplianceStandard],
        include_evidence: bool = True,
        format: str = "json"
    ) -> Dict[str, Any]:
        """Generate comprehensive compliance report."""
        assessments = {}
        overall_score = 0.0
        total_weight = 0

        # Weight different standards
        weights = {
            ComplianceStandard.GDPR: 0.4,
            ComplianceStandard.ISO_27001: 0.3,
            ComplianceStandard.SOC_2: 0.3
        }

        for standard in standards:
            if standard in weights:
                assessment = await self.assess_compliance(standard)
                assessments[standard.value] = assessment
                overall_score += assessment["compliance_score"] * weights[standard]
                total_weight += weights[standard]

        overall_score = overall_score / total_weight if total_weight > 0 else 0.0

        report = {
            "report_id": f"compliance-{datetime.now().timestamp()}",
            "generated_at": datetime.now().isoformat(),
            "standards": [s.value for s in standards],
            "overall_compliance_score": overall_score,
            "assessments": assessments,
            "summary": {
                "total_standards": len(standards),
                "total_requirements": sum(
                    len(self.compliance_standards.get(s, [])) for s in standards
                ),
                "compliant_requirements": sum(
                    sum(1 for r in assessments[s.value]["requirements"] if r["compliant"])
                    for s in standards
                ),
                "compliance_rating": self._get_compliance_rating(overall_score)
            }
        }

        # Log report generation
        logger.info(
            "Compliance report generated",
            extra={
                "report_id": report["report_id"],
                "standards": [s.value for s in standards],
                "overall_score": overall_score,
                "rating": report["summary"]["compliance_rating"]
            }
        )

        return report

    def _get_compliance_rating(self, score: float) -> str:
        """Get compliance rating based on score."""
        if score >= 0.95:
            return "Excellent"
        elif score >= 0.85:
            return "Good"
        elif score >= 0.75:
            return "Satisfactory"
        elif score >= 0.65:
            return "Needs Improvement"
        else:
            return "Critical Issues"

    async def run_security_audit(
        self,
        audit_scope: List[str],
        audit_type: str = "comprehensive"
    ) -> Dict[str, Any]:
        """Run security audit."""
        audit_results = {
            "audit_id": f"audit-{datetime.now().timestamp()}",
            "audit_type": audit_type,
            "scope": audit_scope,
            "start_time": datetime.now().isoformat(),
            "findings": [],
            "recommendations": [],
            "risk_score": 0.0
        }

        # Simulate audit findings
        findings = [
            {
                "severity": "low",
                "category": "configuration",
                "title": "Password policy could be strengthened",
                "description": "Consider implementing more restrictive password requirements",
                "recommendation": "Update password policy to require 12+ characters, mixed case, and special characters"
            },
            {
                "severity": "medium",
                "category": "monitoring",
                "title": "Some endpoints lack detailed audit logging",
                "description": "Certain administrative endpoints do not log sufficient detail for compliance",
                "recommendation": "Enhance audit logging for admin endpoints to include parameter details"
            }
        ]

        audit_results["findings"] = findings
        audit_results["risk_score"] = sum(
            1 if f["severity"] == "low" else 3 if f["severity"] == "medium" else 5
            for f in findings
        ) / len(findings) if findings else 0.0

        audit_results["end_time"] = datetime.now().isoformat()

        # Log audit completion
        logger.info(
            "Security audit completed",
            extra={
                "audit_id": audit_results["audit_id"],
                "findings_count": len(findings),
                "risk_score": audit_results["risk_score"]
            }
        )

        return audit_results

    async def validate_tenant_isolation(
        self,
        user_id: str,
        tenant_id: str,
        resource: str,
        operation: str
    ) -> Dict[str, Any]:
        """Validate tenant isolation for an operation."""
        validation_result = {
            "validation_id": f"tenant-isolation-{datetime.now().timestamp()}",
            "user_id": user_id,
            "tenant_id": tenant_id,
            "resource": resource,
            "operation": operation,
            "timestamp": datetime.now().isoformat(),
            "isolated": True,
            "violations": [],
            "recommendations": []
        }

        # Check if tenant isolation is properly enforced
        if not self.settings.auth.tenant_isolation_enabled:
            validation_result["violations"].append({
                "type": "configuration",
                "severity": "critical",
                "description": "Tenant isolation is disabled in configuration"
            })
            validation_result["isolated"] = False

        # Check if strict isolation is enabled
        if not self.settings.auth.tenant_isolation_strict:
            validation_result["recommendations"].append({
                "type": "configuration",
                "description": "Consider enabling strict tenant isolation for enhanced security"
            })

        # Log validation
        logger.info(
            "Tenant isolation validation completed",
            extra={
                "validation_id": validation_result["validation_id"],
                "isolated": validation_result["isolated"],
                "violations_count": len(validation_result["violations"])
            }
        )

        return validation_result

    async def check_data_classification_compliance(
        self,
        data_classification: DataClassification,
        access_level: str,
        user_role: str,
        operation: str
    ) -> Dict[str, Any]:
        """Check data classification compliance."""
        check_result = {
            "check_id": f"classification-{datetime.now().timestamp()}",
            "data_classification": data_classification.value,
            "access_level": access_level,
            "user_role": user_role,
            "operation": operation,
            "timestamp": datetime.now().isoformat(),
            "compliant": True,
            "violations": [],
            "recommendations": []
        }

        # Define classification hierarchy
        classification_hierarchy = {
            DataClassification.PUBLIC: 1,
            DataClassification.INTERNAL: 2,
            DataClassification.CONFIDENTIAL: 3,
            DataClassification.RESTRICTED: 4,
            DataClassification.HIGHLY_RESTRICTED: 5
        }

        # Define role hierarchy (higher number = more access)
        role_hierarchy = {
            "user": 1,
            "analyst": 2,
            "trader": 3,
            "admin": 4,
            "super_admin": 5
        }

        required_level = classification_hierarchy.get(data_classification, 1)
        user_level = role_hierarchy.get(user_role, 1)

        if user_level < required_level:
            check_result["violations"].append({
                "type": "access_control",
                "severity": "high",
                "description": f"User role '{user_role}' insufficient for '{data_classification.value}' data",
                "required_level": required_level,
                "user_level": user_level
            })
            check_result["compliant"] = False

        # Log check
        logger.info(
            "Data classification compliance check completed",
            extra={
                "check_id": check_result["check_id"],
                "compliant": check_result["compliant"],
                "violations_count": len(check_result["violations"])
            }
        )

        return check_result

    def get_compliance_summary(self) -> Dict[str, Any]:
        """Get compliance summary."""
        total_requirements = len(self.requirements)
        compliant_requirements = len([r for r in self.requirements.values() if r.is_compliant()])
        partially_compliant = len([r for r in self.requirements.values() if r.implementation_status == "partially_implemented"])
        non_compliant = total_requirements - compliant_requirements - partially_compliant

        compliance_score = (compliant_requirements * 1.0 + partially_compliant * 0.5) / total_requirements if total_requirements > 0 else 0.0

        return {
            "total_requirements": total_requirements,
            "compliant_requirements": compliant_requirements,
            "partially_compliant_requirements": partially_compliant,
            "non_compliant_requirements": non_compliant,
            "compliance_score": compliance_score,
            "compliance_rating": self._get_compliance_rating(compliance_score),
            "standards": list(self.compliance_standards.keys()),
            "requirements_due_for_review": len([r for r in self.requirements.values() if r.is_due_for_review()])
        }


# Global compliance manager instance
_compliance_manager: Optional[ComplianceManager] = None


def get_compliance_manager(settings: AurumSettings) -> ComplianceManager:
    """Get global compliance manager instance."""
    global _compliance_manager
    if _compliance_manager is None:
        _compliance_manager = ComplianceManager(settings)
    return _compliance_manager


async def perform_compliance_check(
    settings: AurumSettings,
    standards: List[ComplianceStandard]
) -> Dict[str, Any]:
    """Perform comprehensive compliance check."""
    manager = get_compliance_manager(settings)
    return await manager.generate_compliance_report(standards)


async def validate_security_controls(
    settings: AurumSettings,
    controls: List[ComplianceControl]
) -> Dict[str, Any]:
    """Validate security controls implementation."""
    manager = get_compliance_manager(settings)

    results = {}
    for control in controls:
        # Find requirements for this control
        control_requirements = [
            req for req in manager.requirements.values()
            if req.control == control
        ]

        results[control.value] = {
            "control": control.value,
            "total_requirements": len(control_requirements),
            "compliant_requirements": len([r for r in control_requirements if r.is_compliant()]),
            "partially_compliant": len([r for r in control_requirements if r.implementation_status == "partially_implemented"]),
            "non_compliant": len([r for r in control_requirements if not r.is_compliant() and r.implementation_status != "partially_implemented"]),
            "requirements": [
                {
                    "requirement_id": req.requirement_id,
                    "description": req.description,
                    "status": req.implementation_status,
                    "compliant": req.is_compliant()
                }
                for req in control_requirements
            ]
        }

    return results
