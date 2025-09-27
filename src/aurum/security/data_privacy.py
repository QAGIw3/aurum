"""Data privacy controls for Phase 4 enterprise implementation."""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)


class DataClassification(str, Enum):
    """Data classification levels for privacy controls."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    HIGHLY_RESTRICTED = "highly_restricted"


class PrivacyRegulation(str, Enum):
    """Privacy regulations supported."""
    GDPR = "gdpr"  # EU General Data Protection Regulation
    CCPA = "ccpa"  # California Consumer Privacy Act
    PIPEDA = "pipeda"  # Personal Information Protection and Electronic Documents Act (Canada)
    LGPD = "lgpd"  # Lei Geral de Proteção de Dados (Brazil)
    PDPA = "pdpa"  # Personal Data Protection Act (Singapore/Thailand)


class ConsentType(str, Enum):
    """Types of consent for data processing."""
    EXPLICIT = "explicit"  # Clear affirmative action
    IMPLIED = "implied"    # Reasonable expectation
    OPT_OUT = "opt_out"   # Default with opt-out option
    LEGITIMATE_INTEREST = "legitimate_interest"


class DataRetentionPolicy(str, Enum):
    """Data retention policies."""
    IMMEDIATE_DELETE = "immediate_delete"
    NINETY_DAYS = "90_days"
    ONE_YEAR = "1_year"
    THREE_YEARS = "3_years"
    SEVEN_YEARS = "7_years"  # Compliance requirement
    TEN_YEARS = "10_years"   # Extended compliance
    INDEFINITE = "indefinite"


@dataclass
class PIIField:
    """Personal Identifiable Information field definition."""
    
    field_name: str
    field_type: str  # email, phone, ssn, name, address, etc.
    classification: DataClassification
    pseudonymize: bool = False
    encrypt: bool = False
    mask_in_logs: bool = True
    retention_policy: DataRetentionPolicy = DataRetentionPolicy.SEVEN_YEARS
    
    def should_be_redacted(self) -> bool:
        """Check if field should be redacted in logs/exports."""
        return self.classification in [
            DataClassification.RESTRICTED,
            DataClassification.HIGHLY_RESTRICTED
        ]


@dataclass
class ConsentRecord:
    """Data processing consent record."""
    
    user_id: str
    tenant_id: str
    consent_type: ConsentType
    purpose: str
    regulation: PrivacyRegulation
    granted_at: datetime
    expires_at: Optional[datetime] = None
    withdrawn_at: Optional[datetime] = None
    consent_details: Dict[str, Any] = field(default_factory=dict)
    
    def is_valid(self) -> bool:
        """Check if consent is still valid."""
        if self.withdrawn_at:
            return False
        if self.expires_at and datetime.now() > self.expires_at:
            return False
        return True


@dataclass
class DataProcessingRecord:
    """Record of data processing activity."""
    
    record_id: str
    user_id: str
    tenant_id: str
    data_types: List[str]
    purpose: str
    legal_basis: str
    processed_at: datetime
    processor: str  # System/service that processed the data
    retention_until: datetime
    cross_border_transfer: bool = False
    third_party_sharing: bool = False
    
    def is_retention_expired(self) -> bool:
        """Check if data retention period has expired."""
        return datetime.now() > self.retention_until


class DataPrivacyManager:
    """Enhanced data privacy manager for enterprise compliance."""

    def __init__(self):
        self.pii_fields: Dict[str, PIIField] = {}
        self.consent_records: Dict[str, List[ConsentRecord]] = {}
        self.processing_records: List[DataProcessingRecord] = []
        self.data_subject_requests: List[Dict[str, Any]] = []
        self._initialize_default_pii_fields()

    def _initialize_default_pii_fields(self) -> None:
        """Initialize default PII field definitions."""
        default_fields = [
            PIIField("email", "email", DataClassification.CONFIDENTIAL, 
                    pseudonymize=True, encrypt=True),
            PIIField("phone", "phone", DataClassification.CONFIDENTIAL,
                    pseudonymize=True, encrypt=True),
            PIIField("first_name", "name", DataClassification.INTERNAL),
            PIIField("last_name", "name", DataClassification.INTERNAL),
            PIIField("full_name", "name", DataClassification.INTERNAL),
            PIIField("address", "address", DataClassification.CONFIDENTIAL,
                    pseudonymize=True, encrypt=True),
            PIIField("ssn", "ssn", DataClassification.HIGHLY_RESTRICTED,
                    pseudonymize=True, encrypt=True),
            PIIField("tax_id", "tax_id", DataClassification.HIGHLY_RESTRICTED,
                    pseudonymize=True, encrypt=True),
            PIIField("ip_address", "ip_address", DataClassification.INTERNAL,
                    mask_in_logs=True),
            PIIField("user_agent", "user_agent", DataClassification.INTERNAL)
        ]
        
        for field in default_fields:
            self.pii_fields[field.field_name] = field

    def register_pii_field(self, field: PIIField) -> None:
        """Register a new PII field definition."""
        self.pii_fields[field.field_name] = field
        logger.info(f"Registered PII field: {field.field_name} ({field.classification})")

    def classify_data(self, data: Dict[str, Any]) -> Dict[str, DataClassification]:
        """Classify data fields based on registered PII definitions."""
        classifications = {}
        
        for field_name, value in data.items():
            if field_name in self.pii_fields:
                classifications[field_name] = self.pii_fields[field_name].classification
            else:
                # Default classification for unknown fields
                classifications[field_name] = DataClassification.INTERNAL
                
        return classifications

    def pseudonymize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Pseudonymize sensitive data fields."""
        pseudonymized = data.copy()
        
        for field_name, value in data.items():
            if field_name in self.pii_fields:
                pii_field = self.pii_fields[field_name]
                if pii_field.pseudonymize and value:
                    pseudonymized[field_name] = self._pseudonymize_value(str(value))
                    
        return pseudonymized

    def _pseudonymize_value(self, value: str) -> str:
        """Generate pseudonymized version of a value."""
        # Use SHA-256 hash for pseudonymization
        return hashlib.sha256(value.encode()).hexdigest()[:16]

    def mask_sensitive_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Mask sensitive data for logging/display."""
        masked = data.copy()
        
        for field_name, value in data.items():
            if field_name in self.pii_fields:
                pii_field = self.pii_fields[field_name]
                if pii_field.mask_in_logs and value:
                    masked[field_name] = self._mask_value(str(value), pii_field.field_type)
                    
        return masked

    def _mask_value(self, value: str, field_type: str) -> str:
        """Mask a value based on its type."""
        if field_type == "email":
            parts = value.split("@")
            if len(parts) == 2:
                return f"{parts[0][:2]}***@{parts[1]}"
        elif field_type == "phone":
            return f"***-***-{value[-4:]}" if len(value) >= 4 else "***"
        elif field_type == "ssn":
            return f"***-**-{value[-4:]}" if len(value) >= 4 else "***"
        elif field_type in ["name", "address"]:
            return f"{value[:2]}***" if len(value) > 2 else "***"
        
        # Default masking
        return "***"

    def record_consent(self, consent: ConsentRecord) -> None:
        """Record user consent for data processing."""
        user_key = f"{consent.user_id}:{consent.tenant_id}"
        if user_key not in self.consent_records:
            self.consent_records[user_key] = []
        
        self.consent_records[user_key].append(consent)
        logger.info(f"Recorded consent: {consent.user_id} for {consent.purpose}")

    def check_consent(self, user_id: str, tenant_id: str, purpose: str) -> bool:
        """Check if user has valid consent for specific data processing purpose."""
        user_key = f"{user_id}:{tenant_id}"
        if user_key not in self.consent_records:
            return False
        
        for consent in self.consent_records[user_key]:
            if consent.purpose == purpose and consent.is_valid():
                return True
                
        return False

    def withdraw_consent(self, user_id: str, tenant_id: str, purpose: str) -> bool:
        """Withdraw user consent for specific purpose."""
        user_key = f"{user_id}:{tenant_id}"
        if user_key not in self.consent_records:
            return False
        
        withdrawn = False
        for consent in self.consent_records[user_key]:
            if consent.purpose == purpose and consent.is_valid():
                consent.withdrawn_at = datetime.now()
                withdrawn = True
                
        if withdrawn:
            logger.info(f"Withdrawn consent: {user_id} for {purpose}")
            
        return withdrawn

    def record_data_processing(self, record: DataProcessingRecord) -> None:
        """Record data processing activity."""
        self.processing_records.append(record)
        logger.info(f"Recorded data processing: {record.record_id} for {record.user_id}")

    def get_user_data_processing(self, user_id: str, tenant_id: str) -> List[DataProcessingRecord]:
        """Get all data processing records for a user."""
        return [
            record for record in self.processing_records
            if record.user_id == user_id and record.tenant_id == tenant_id
        ]

    def handle_data_subject_request(self, request_type: str, user_id: str, 
                                  tenant_id: str, details: Dict[str, Any]) -> Dict[str, Any]:
        """Handle data subject rights requests (GDPR Article 15-22)."""
        request_id = f"dsr_{int(datetime.now().timestamp())}_{user_id}"
        
        request = {
            "request_id": request_id,
            "request_type": request_type,  # access, rectification, erasure, portability
            "user_id": user_id,
            "tenant_id": tenant_id,
            "requested_at": datetime.now().isoformat(),
            "status": "pending",
            "details": details
        }
        
        self.data_subject_requests.append(request)
        
        # Auto-process some request types
        if request_type == "access":
            return self._process_data_access_request(user_id, tenant_id, request_id)
        elif request_type == "erasure":
            return self._process_data_erasure_request(user_id, tenant_id, request_id)
        
        return request

    def _process_data_access_request(self, user_id: str, tenant_id: str, 
                                   request_id: str) -> Dict[str, Any]:
        """Process data access request (Right to Access - GDPR Article 15)."""
        # Get user's processing records
        processing_records = self.get_user_data_processing(user_id, tenant_id)
        
        # Get user's consent records
        user_key = f"{user_id}:{tenant_id}"
        consent_records = self.consent_records.get(user_key, [])
        
        response = {
            "request_id": request_id,
            "user_id": user_id,
            "tenant_id": tenant_id,
            "data_categories": list(set([
                data_type for record in processing_records 
                for data_type in record.data_types
            ])),
            "processing_purposes": list(set([
                record.purpose for record in processing_records
            ])),
            "consent_records": [
                {
                    "purpose": consent.purpose,
                    "granted_at": consent.granted_at.isoformat(),
                    "status": "active" if consent.is_valid() else "withdrawn/expired"
                }
                for consent in consent_records
            ],
            "retention_periods": list(set([
                record.retention_until.isoformat() for record in processing_records
            ])),
            "third_party_sharing": any(record.third_party_sharing for record in processing_records),
            "cross_border_transfers": any(record.cross_border_transfer for record in processing_records)
        }
        
        return response

    def _process_data_erasure_request(self, user_id: str, tenant_id: str, 
                                    request_id: str) -> Dict[str, Any]:
        """Process data erasure request (Right to be Forgotten - GDPR Article 17)."""
        # In a real implementation, this would coordinate with data stores
        # to actually delete the user's data
        
        response = {
            "request_id": request_id,
            "user_id": user_id,
            "tenant_id": tenant_id,
            "status": "scheduled",
            "estimated_completion": (datetime.now() + timedelta(days=30)).isoformat(),
            "data_sources_to_process": [
                "user_profiles",
                "transaction_history", 
                "audit_logs",
                "analytics_data"
            ],
            "exceptions": [
                "Legal compliance data (7-year retention required)",
                "Financial transaction records (regulatory requirement)"
            ]
        }
        
        return response

    def get_retention_expired_data(self) -> List[DataProcessingRecord]:
        """Get data processing records that have exceeded retention period."""
        return [
            record for record in self.processing_records
            if record.is_retention_expired()
        ]

    def generate_privacy_impact_assessment(self, tenant_id: str) -> Dict[str, Any]:
        """Generate Privacy Impact Assessment report."""
        tenant_records = [
            record for record in self.processing_records
            if record.tenant_id == tenant_id
        ]
        
        data_types = set()
        purposes = set()
        cross_border_count = 0
        third_party_count = 0
        
        for record in tenant_records:
            data_types.update(record.data_types)
            purposes.add(record.purpose)
            if record.cross_border_transfer:
                cross_border_count += 1
            if record.third_party_sharing:
                third_party_count += 1
        
        # Calculate risk score
        risk_factors = []
        risk_score = 0
        
        if any("highly_restricted" in record.data_types for record in tenant_records):
            risk_factors.append("Processing highly restricted data")
            risk_score += 30
        
        if cross_border_count > 0:
            risk_factors.append(f"Cross-border data transfers: {cross_border_count}")
            risk_score += 20
        
        if third_party_count > 0:
            risk_factors.append(f"Third-party data sharing: {third_party_count}")
            risk_score += 15
        
        return {
            "tenant_id": tenant_id,
            "assessment_date": datetime.now().isoformat(),
            "data_types_processed": list(data_types),
            "processing_purposes": list(purposes),
            "total_processing_activities": len(tenant_records),
            "cross_border_transfers": cross_border_count,
            "third_party_sharing": third_party_count,
            "risk_score": min(risk_score, 100),
            "risk_factors": risk_factors,
            "recommendation": "high" if risk_score > 60 else "medium" if risk_score > 30 else "low"
        }


# Global privacy manager instance
privacy_manager = DataPrivacyManager()